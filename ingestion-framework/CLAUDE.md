# CLAUDE.md

Instruções para assistentes de IA que forem escrever código **neste** framework de ingestão.

## Princípio geral

Este repositório é um arquivo histórico de código de produção. **Não refatore, não "corrija" e não modernize nada** a menos que seja explicitamente pedido. O objetivo é o oposto: entender o padrão existente e reproduzi-lo fielmente ao escrever coisas novas.

Vários comportamentos aqui parecem erros e não são — são o mecanismo. Outros são erros de verdade, e estão catalogados em [docs/known-issues.md](docs/known-issues.md). Leia esse arquivo antes de mexer em qualquer coisa, para não "consertar" o que é intencional nem replicar o que é defeito.

---

## Ambiente

- Todo o código roda em **notebooks Databricks**. `spark` e `dbutils` são globais da plataforma — **nunca** adicione `SparkSession.builder.getOrCreate()` nem importe `dbutils`.
- Os arquivos deste repositório não são módulos Python importáveis: eles não têm `import` nenhum. Todos os imports vivem em um único notebook (`imports/imports.py`), carregado com `%run` antes das classes. Ao criar uma classe nova, **não** adicione imports no topo do arquivo; adicione ao `imports` se faltar algo.
- Projeto GCP: `data-prod-295413`, hardcoded. Mantenha assim em código novo deste repositório.

---

## Como criar um extractor novo

1. Herde de `DataExtractor` e implemente `extract(self, export_date, reference_date, table, table_config)`.
2. Retorne sempre uma tupla de três elementos:
   ```python
   return df, dbfs_file_path, original_source_name
   ```
   - **Fonte de arquivo ou API:** retorne o caminho DBFS e o nome original. O pipeline sobe esse arquivo para a camada raw.
   - **Fonte de banco de dados:** retorne `df, None, None`. O pipeline pula a camada raw.
3. Credenciais vêm do Secret Manager, no formato exato:
   ```python
   self.sm_client = secretmanager.SecretManagerServiceClient()
   sm_user = self.sm_client.access_secret_version(
       request={"name": "projects/data-prod-295413/secrets/{}/versions/1".format(sm_user_secret_id)}
   ).payload.data.decode("UTF-8")
   ```
4. Arquivo temporário segue o padrão de três linhas:
   ```python
   temp_data_file_name = self.generate_temp_file_name()   # devolve '/tmpXXXX'
   dbfs_folder = '/tmp'
   dbfs_file_path = dbfs_folder + temp_data_file_name     # '/tmp/tmpXXXX'
   full_file_path = '/dbfs' + dbfs_file_path              # '/dbfs/tmp/tmpXXXX'
   ```
   Escreva em `full_file_path` (mount FUSE), mas passe `dbfs_file_path` para o Spark. O `dir='/'` dentro de `generate_temp_file_name` existe justamente para o nome sair começando com `/tmp`; não altere.
5. Registre o extractor na `DataExtractorFactory`, lendo os parâmetros de `ingestion_config['sec']`.
6. Se a fonte suportar reprocessamento a partir do lake, implemente também `extract_from_raw(self, raw_path, table_config)`. Use `SharepointDataExtractor` como modelo — é o único que tem.

### Modelo de referência

Ao escrever um extractor novo, siga o **`SharepointDataExtractor`**. É o mais completo e o único que valida o contrato do dado de entrada:

- confere a quantidade de colunas do arquivo contra o config e **levanta exceção** se divergir;
- confere os nomes das colunas nos dois sentidos (arquivo → config e config → arquivo) e levanta exceção;
- detecta e normaliza o encoding antes de ler.

Os outros extractors não fazem nenhuma dessas verificações. Prefira o padrão do SharePoint.

---

## Convenções obrigatórias

### Log

`print` com timestamp manual, nunca a biblioteca `logging`:

```python
print('INFO - {} - Mensagem'.format(str(pydt.datetime.now())))
print('ERROR - {} - Mensagem'.format(str(pydt.datetime.now())))
```

### Nomenclatura de arquivos

| Item | Padrão | Exemplo |
|---|---|---|
| Notebook de execução | `ingestion_<project>_<service>_<database>` | `ingestion_avenue_sharepoint_teamdata_itau` |
| Config | `conf_` + nome do notebook | `conf_ingestion_avenue_sharepoint_teamdata_itau` |
| Classe extractor | `<Fonte>DataExtractor` | `SharepointDataExtractor` |

### Normalização de nomes de coluna — **regra crítica**

O `IngestionPipeline.ingest_table` renomeia todas as colunas depois da extração, nesta ordem:

1. remove acentos (`strip_accents`);
2. troca sequências de espaço por `_`;
3. **remove** (não substitui) tudo que não for `[a-zA-Z0-9_]`;
4. prefixa com `_` se o nome começar com dígito.

Isso significa que, no config:

- **`schema` / `header` usam os nomes do arquivo de origem**, com espaços e pontuação;
- **`primary_key` usa os nomes já normalizados.**

Exemplos reais deste repositório:

| Nome na origem | Nome normalizado |
|---|---|
| `UNIQUE PROGRAM ID` | `UNIQUE_PROGRAM_ID` |
| `TRANSACTION CODE/TYPE` | `TRANSACTION_CODETYPE` |
| `TRANSACTION DATE/TIME` | `TRANSACTION_DATETIME` |
| `E-MAIL` | `EMAIL` |

Repare que a barra e o hífen **somem**, não viram underscore. Errar isso faz o `primary_key` não bater com nenhuma coluna e a deduplicação do layer2 quebrar.

### Datas

Nos notebooks, sempre:

```python
export_date    = pydt.datetime.today()
reference_date = export_date - pydt.timedelta(days=1)
```

Para reprocessar um dia específico, o padrão do repositório é comentar a linha do `today()` e fixar a data logo acima. Para reprocessar um intervalo, use o laço de `dates_export` de `ingestion/bigquery/ingestion_avenue_bigquery_table.py`, **em ordem crescente** — o modo `update` depende do staging do dia anterior já existir.

### Falha no notebook

`IngestionPipeline.run()` **nunca levanta exceção**: ele imprime e retorna normalmente mesmo com tabelas quebradas. Um job Databricks que só chama `run()` termina como sucesso independentemente do resultado.

Se o job precisa falhar de verdade, replique o padrão do notebook de SharePoint:

```python
for item in pipeline.run_results:
    display(pipeline.run_results[item])
    break

if pipeline.run_results[item][0]['result'] != 'success':
    raise Exception("Table {} ingestion failed".format(table_name))
```

(Esse `[0]` só é correto porque aquele notebook roda uma tabela por vez.)

---

## Como criar um config novo

Estrutura mínima:

```python
config = {
    "project_id": "…",
    "service"   : "…",   # decide qual extractor a Factory devolve
    "type"      : "…",   # subtipo; alguns extractors usam, outros ignoram
    "instance"  : "…",
    "database"  : "…",
    "sec"       : { … },  # bloco livre, interpretado por cada extractor
    "tables"    : { "<nome_da_tabela>": { … } }
}
```

O caminho no GCS é derivado assim:

```
<prefix><service>/<project_id>/<instance>/<database>/<table>
```

com `-`, `.` e `/` trocados por `_` em cada componente. O prefixo é vazio no staging, `layer1/` no datalake e `layer2/ingestion_latest_snapshot/` no snapshot.

Antes de escrever um config, consulte [docs/table-config-reference.md](docs/table-config-reference.md): **cada extractor lê um subconjunto diferente das chaves**, e há chaves que aparecem em configs existentes mas não são lidas por ninguém. Dois exemplos que costumam confundir:

- o SharePoint usa `header`; Galileo e BCB usam `schema` — mesma estrutura, chave diferente;
- `changed_range` é interpretado com a unidade (`days` / `Months`) só no BCB; MongoDB e BigQuery ignoram a unidade e tratam tudo como dias.

---

## O que não fazer

- Não troque `print` por `logging`.
- Não adicione `import` nos arquivos de classe.
- Não substitua o `dir='/'` do `generate_temp_file_name`.
- Não "simplifique" o truque do `'removetable'` no cálculo de `data_origin` — ele existe para reaproveitar `get_parquet_path` sem a parte da tabela.
- Não altere as versões fixas dos secrets (`versions/1`, `/2`, `/3`) sem confirmar qual versão está ativa no Secret Manager.
- Não generalize o `MongoDBDataExtractor`: apesar do nome, ele é específico da coleção `CustomerAccount`/`AccountMoviment`. Para outra coleção, escreva um extractor novo.
