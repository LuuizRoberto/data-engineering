# Pipeline — Google Analytics 4 (via BigQuery)

Exportação diária do GA4 para o BigQuery, reingerida no data lake.

| | |
|---|---|
| **Notebook** | [`ingestion/bigquery/ingestion_avenue_bigquery_table.py`](../../ingestion/bigquery/ingestion_avenue_bigquery_table.py) |
| **Config** | [`ingestion-conf/conf_ingestion_avenue_bigquery_table.py`](../../ingestion-conf/conf_ingestion_avenue_bigquery_table.py) |
| **Extractor** | `BigQueryDataExtractor` (`ds_type = 'googleanalytics'`) |
| **Origem** | `data-prod-295413.analytics_328012517.*` |
| **Autenticação** | credencial da própria compute do Databricks |
| **Caminho no lake** | `bigquery/avenue/ga4/analytics/<tabela>` |

## Tabelas

| Tabela | `endpoint` (tabela no BigQuery) | `primary_key` |
|---|---|---|
| `events` | `events` → `events_YYYYMMDD` | `event_date, event_timestamp, event_name, event_params` |
| `user` | `pseudonymous_users` → `pseudonymous_users_YYYYMMDD` | `pseudo_user_id, stream_id, user_info` |

Ambas: `extract_mode: update`, `changed_date` e `changed_range` nulos, sem `latest_snapshot_mode`.

## Fluxo

1. Como `ds_type == 'googleanalytics'`, o sufixo da tabela diária vem de `reference_date` (`%Y%m%d`) — ou seja, o dia anterior ao `export_date`.
2. Leitura via conector `spark.read.format('bigquery')`, apontando para `analytics_328012517.<endpoint>_<YYYYMMDD>`.
3. Tipagem por listas de campos:
   - `struct_fields` → serializados com `to_json` (é assim que `event_params`, `device`, `geo`, `items` etc. entram no lake, como strings JSON);
   - `integer_fields` → `event_date` (e `occurrence_date`, `last_updated_date` em `user`);
   - `float_fields` → `event_value_in_usd`;
   - **todo o resto vira string**, inclusive `event_timestamp`.
4. Retorna `(df, None, None)` — **sem camada raw**.
5. Layer1: anti-join por `dl_hash_key` contra o staging de `reference_date`.

## Reprocessamento

Este é o notebook que traz o padrão de backfill do framework:

```python
# days_range = config['modification_date'].split(" ")
days_range = ["9"]

enddate = pydt.datetime.today()
startdate = enddate - pydt.timedelta(days=int(days_range[0]))
...
for date in dates_export:
    export_date = pydt.datetime.strptime(date, '%Y-%m-%d')
    reference_date = export_date - pydt.timedelta(days=1)
    pipeline = IngestionPipeline(config, bucket_config)
    pipeline.run(export_date, reference_date)
```

A chave `modification_date: '3 days'` do config existe para alimentar esse laço — a linha que a lê está comentada e o intervalo foi fixado em 9 dias na mão.

A motivação é a natureza do GA4: os dados de um dia continuam chegando depois do fechamento, então reprocessar os últimos dias é o normal. A ordem crescente das datas é obrigatória, porque cada dia depende do staging do anterior.

## Especificidades

- É o único extractor que **usa o valor** de `changed_date` — como nome da coluna do `WHERE`. Neste config ele é nulo, então o branch não é exercitado.
- **Suspeita registrada:** o filtro incremental do branch genérico pode nunca ter sido aplicado, por dois motivos (opção `sqlQuery` em vez de `query`, e query sem crases num project id com hífen). Ver [../known-issues.md](../known-issues.md), seção 5. Não afeta o GA4, que lê a tabela diária inteira por definição.
- `event_timestamp` compõe o `primary_key` e é convertido para string pelo cast padrão. `event_params` também compõe o `primary_key` e vira uma string JSON — a janela de deduplicação do layer2 particiona por esse JSON inteiro.
- `parent_columns` está no config e não é lido por nenhum extractor.
- O notebook precisa de `pip install db-dtypes` antes de carregar as classes.
- Diferença de opções entre os dois branches do extractor: o de GA4 passa `fetchSize`, o genérico passa `viewsEnabled`.
