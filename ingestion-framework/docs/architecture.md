# Arquitetura

## Visão geral

O framework tem duas peças abstratas e um mecanismo de composição:

- **`DataExtractor`** sabe falar com *uma* fonte e devolver um DataFrame Spark.
- **`Pipeline`** sabe persistir DataFrames nas camadas do lake e registrar tabelas no BigQuery.
- **`DataExtractorFactory`** liga os dois, escolhendo o extractor a partir de `config['service']`.

O que é ingerido, de onde e como não está no código — está no **config**, um dicionário Python. O código é genérico por fonte; o config é específico por tabela.

---

## As camadas

### raw

O arquivo **exatamente como veio da origem**, sem parse.

```
gs://<raw_bucket>/<service>/<project_id>/<instance>/<database>/<table>/dl_export_date=YYYY-MM-DD/<nome_original>
```

Só existe para fontes de arquivo e API. Extractors de banco retornam `df, None, None` e o pipeline pula esta camada. O nome original tem as barras trocadas por `_`.

### staging

Snapshot **completo do que foi extraído naquele dia**, em parquet, com duas colunas acrescentadas pelo pipeline:

- **`dl_hash_key`** — SHA-256 da concatenação de todas as colunas. É a impressão digital da linha.
- **`dl_export_datetime`** — timestamp da execução.

```
gs://<staging_bucket>/<service>/<project_id>/<instance>/<database>/<table>/dl_export_date=YYYY-MM-DD
```

Escrita em modo `overwrite` na partição do dia, seguida de uma releitura do caminho — o padrão do framework para materializar o resultado e cortar a linhagem do Spark.

### layer1

O que efetivamente entra no data lake, conforme o `extract_mode` da tabela:

| `extract_mode` | Comportamento |
|---|---|
| `append` | `df_lake = df` — a partição do dia recebe tudo o que veio |
| `update` | anti-join contra o staging de `reference_date` por `dl_hash_key`; só linhas novas ou alteradas seguem |

```
gs://<datalake_bucket>/layer1/<service>/<project_id>/<instance>/<database>/<table>/dl_export_date=YYYY-MM-DD
```

O modo `update` cria uma **dependência entre dias**: a execução de hoje lê o staging de ontem. Se ontem não rodou, o caminho não existe e a tabela falha. É por isso que reprocessar um intervalo exige percorrer as datas em ordem crescente.

### layer2 — latest snapshot

Gerado por `IngestionLatestSnapshotPipeline` a partir do layer1 **inteiro** (todas as partições, com `mergeSchema`).

| `latest_snapshot_mode` | Comportamento |
|---|---|
| `append` | mantém apenas a maior `dl_export_date` |
| `update` | `row_number` particionado por `primary_key`, ordenado por `dl_export_datetime` desc, filtra `= 1` |
| `full` | não faz tratamento — grava o layer1 completo |
| *ausente* | usa o `extract_mode` como se fosse `latest_snapshot_mode` |

```
gs://<datalake_bucket>/layer2/ingestion_latest_snapshot/<service>/<project_id>/<instance>/<database>/<table>
```

Sem partição, sobrescrito por completo a cada execução.

O serviço `datastore` tem tratamento especial: a janela é particionada por `key_name` e `key_id` em vez do `primary_key`.

Atenção ao modo `full`: o nome do dataset (`…_latest_snapshot`) sugere "estado atual", mas `full` grava **todo o histórico** do layer1. É o caso das tabelas de SharePoint.

---

## Tabelas no BigQuery

Cada camada registra uma tabela externa apontando para o parquet no GCS. O dataset é o nome do bucket com `-` trocado por `_`; o layer2 acrescenta `_latest_snapshot`.

| Camada | Dataset | Particionamento Hive |
|---|---|---|
| staging | `avenue_datalake_staging` | `CUSTOM`, `/{dl_export_date:DATE}` |
| layer1 | `avenue_datalake` | `CUSTOM`, `/{dl_export_date:DATE}` |
| layer2 | `avenue_datalake_latest_snapshot` | nenhum |
| resultados | `avenue_databricks` | `CUSTOM`, `/{export_date:DATE}` |

**A evolução de schema é aditiva.** No `IngestionPipeline`, se a tabela já existe, apenas colunas novas são adicionadas via patch; mudança de tipo ou remoção de coluna passa despercebida. No `IngestionLatestSnapshotPipeline` a tabela é **dropada e recriada** a cada execução — o que descarta permissões e metadados.

---

## Ciclo de vida de uma execução

```
run(export_date, reference_date, run_tables=[], except_tables=[],
    from_raw_file=False, from_staging=False)
│
├─ gera run_uuid
├─ resolve a lista de tabelas (run_tables ∖ except_tables)
│
├─ ThreadPoolExecutor — uma thread por tabela
│   └─ ingest_table
│       ├─ escolhe a origem:
│       │   from_raw_file → extract_from_raw(raw_path, table_config)
│       │   from_staging  → extract_from_staging(staging_path)
│       │   padrão        → extract(export_date, reference_date, table, table_config)
│       ├─ sobe o arquivo original para raw (se houver)
│       ├─ normaliza os nomes de coluna
│       └─ save_dataframe_to_storage → staging → BigQuery → layer1 → BigQuery
│
├─ coleta os resultados de cada future
├─ persist_pipeline_run_result → pipeline_result/ + tabela no BigQuery
└─ imprime o status final  ← nunca levanta exceção
```

O `ThreadPoolExecutor` é criado **sem `max_workers`**: o paralelismo contra a fonte é o default do Python (`min(32, cpu+4)`), não uma decisão explícita.

### Normalização dos nomes de coluna

Entre a extração e a persistência, toda coluna é renomeada:

```python
new_col_name = re.sub(r'[\s]+', '_', self.strip_accents(col))
new_col_name = re.sub(r'[^a-zA-Z0-9_]+', '', new_col_name)
new_col_name = re.sub(r'^([0-9])', r'_\1', new_col_name)
```

Acento sai, espaço vira `_`, todo o resto da pontuação é **removido**, e nome começando com dígito ganha `_` na frente. É por isso que `TRANSACTION CODE/TYPE` vira `TRANSACTION_CODETYPE` e `E-MAIL` vira `EMAIL`.

Consequência direta: no config, `schema`/`header` usam os nomes da origem, e `primary_key` usa os nomes já normalizados.

---

## Registro de execuções

Cada execução grava uma linha por tabela:

```
gs://<databricks_bucket>/pipeline_result/layer1/ingestion            (particionado por export_date)
gs://<databricks_bucket>/pipeline_result/layer2/ingestion_latest_snapshot
```

Campos: `run_id`, `table`, `result`, `message`, `start_time`, `end_time`, `data_origin`, `reference_date`, `export_date`, `table_config`.

`result` é `success`, `failed` ou `warning` (este último só no layer2, quando o dado foi gravado mas a criação da tabela no BigQuery falhou). Em caso de falha, `message` carrega o traceback completo.

Este é o único registro durável de saúde do pipeline — o `run()` não propaga erro para o job. Ver [known-issues.md](known-issues.md).
