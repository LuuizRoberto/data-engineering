# Referência de configuração

Todas as chaves de config observadas neste repositório, e **quem realmente as lê**.

---

## Nível raiz

| Chave | Lida por | Efeito |
|---|---|---|
| `project_id` | pipelines, Factory (datastore) | 2º componente do caminho no GCS |
| `service` | Factory, pipelines | **escolhe o extractor**; 1º componente do caminho; `datastore` muda a regra do layer2 |
| `instance` | pipelines, Factory (ftp) | 3º componente do caminho; desempata os três extractors de `ftp` |
| `database` | pipelines, extractor de Mongo | 4º componente do caminho |
| `type` | extractors de BigQuery, Mongo, GCS, FTP Apex | subtipo da fonte (`googleanalytics`, `mongodb`, `olinda`, `csv`, …) |
| `sec` | Factory | bloco de conexão/credencial, interpretado por extractor |
| `tables` | pipelines | dicionário `{nome_da_tabela: table_config}` |
| `mult_files` | `IngestionPipeline`, Factory | se `True` e `service == 'gcs'`, usa `GcsDataExtractorMultFiles` e passa o bucket raw ao `extract` |
| `namespace` | Factory (datastore) | exigido por `DatastoreDataExtractor` |
| `modification_date` | **nenhuma classe** | lida pelo notebook do GA4 para montar o intervalo de reprocessamento |

`type` também é lido pelo `IngestionLatestSnapshotPipeline` (`ds_type = self.ingestion_config['type']`), mas o valor nunca é usado.

---

## Bloco `sec` por serviço

Conforme a `DataExtractorFactory`. Chave ausente causa `KeyError` na construção do pipeline.

| `service` | Chaves de `sec` exigidas |
|---|---|
| `s3` | `sm_user_secret_id`, `sm_pwd_secret_id`, `instance` |
| `jira` | `sm_user_secret_id`, `sm_token_secret_id`, `instance` |
| `salesforce` | `sm_user_secret_id`, `sm_pwd_secret_id`, `sm_token_secret_id`, `instance`, `client_id`, `version` |
| `cloudsql` | `sm_user_secret_id`, `sm_pwd_secret_id`, `host`, `db_name`, `port` (+ `type` da raiz) |
| `hiplatform` | `sm_user_secret_id`, `sm_pwd_secret_id`, `sm_token_secret_id`, `host` |
| `facebook` | `sm_user_secret_id`, `sm_pwd_secret_id`, `sm_token_secret_id` |
| `gcs` | `instance` (+ `type` da raiz) |
| `myprofit` | `sm_user_secret_id`, `sm_pwd_secret_id`, `host` |
| `nasdaq` | `sm_token_secret_id`, `host` |
| `apex` | `sm_user_secret_id`, `sm_pwd_secret_id`, `host` |
| `datastore` | — (usa `project_id` e `namespace` da raiz) |
| `ftp` + `instance: apex` | `sm_user_secret_id`, `sm_pwd_secret_id`, `sm_pwd_zipfile_secret_id`, `host` (+ `type`) |
| `ftp` + `instance: thales` | `sm_user_secret_id`, `sm_pwd_secret_id`, `sm_token_secret_id`, `host`, `ssh_port` |
| `ftp` + `instance: marketingcloud` | `host`, `port`, `sm_user_secret_id`, `sm_pwd_secret_id` |
| `sharepoint` | `sm_user_secret_id`, `sm_pwd_secret_id`, `site_url` |
| `gupy` | `sm_token_secret_id`, `host` |
| `cognito` | `sm_user_secret_id`, `sm_pwd_secret_id`, `user_pool_id` |
| `mongodb` | `sm_user_secret_id`, `sm_pwd_secret_id` (+ `database`, `instance`, `service`, `type` da raiz) |
| `bcb` | `host` |
| `fedfunds` | `host`, `sm_token_secret_id` |
| `bigquery` | `host` (é o **project id**) (+ `type`) |
| `appsflyer` | `host`, `sm_token_secret_id` |
| `galileo` | `instance` (bucket GCS), `project_id`, `sm_pwd_secret_id`, `sm_private_key` |

`host` é polimórfico: URL de API no BCB, project id no BigQuery, endereço de servidor no FTP.

**Serviço não reconhecido** faz a Factory retornar `None` sem erro. A falha só aparece adiante, como `AttributeError: 'NoneType' object has no attribute 'extract'`.

---

## `table_config` — chaves comuns

| Chave | Lida por | Efeito |
|---|---|---|
| `extract_mode` | `IngestionPipeline` | `update` (anti-join por hash) ou `append` (tudo). Outro valor causa `NameError` interno, reportado como falha |
| `latest_snapshot_mode` | `IngestionLatestSnapshotPipeline` | `update`, `append` ou `full`. **Sobrepõe** o `extract_mode` no layer2 |
| `primary_key` | `IngestionLatestSnapshotPipeline` | colunas da deduplicação, separadas por vírgula, com os **nomes já normalizados**. `None` só é seguro com modo `full` |
| `endpoint` | todos os extractors | caminho/URL/coleção/tabela na origem; aceita placeholders |
| `changed_date` | BigQuery (valor), BCB e Mongo (só a existência) | nome da coluna do filtro incremental |
| `changed_range` | BCB, Mongo, BigQuery | tamanho da janela, no formato `"<n> <unidade>"` |
| `decimal_target_type` | ambos os pipelines | passado ao BigQuery como `decimal_target_types` da tabela externa |
| `exclude_columns` | Galileo | colunas descartadas após a leitura |

### Atenção ao `changed_range`

Só o **BCB** interpreta a unidade:

```python
if   changed_range[1] == 'days':   timedelta(days=n)
elif changed_range[1] == 'Months': relativedelta(months=n)   # M maiúsculo
```

MongoDB e BigQuery fazem sempre `timedelta(days=n)` e ignoram a unidade — `"12 Months"` num config de Mongo significa **12 dias**.

---

## `table_config` — schema e tipos

| Chave | Lida por | Observação |
|---|---|---|
| `schema` | Galileo (usa), BCB (constrói e **descarta**) | `[{field, type}]` com os nomes **da origem** |
| `header` | SharePoint | mesma estrutura, chave diferente |
| `delimiter` | Galileo, SharePoint | separador do CSV |
| `has_header` | SharePoint | se `False`, o cabeçalho do config é inserido no arquivo |
| `replace_header` | Galileo | substitui a primeira linha pelos nomes do `schema` |
| `ds_type` | SharePoint | `csv` ou `xlsx` — note que é por tabela, distinto do `type` da raiz |
| `filename_date_field` | SharePoint | nome do placeholder de data no `endpoint` |
| `filename_date_format` | SharePoint | formato `strftime` aplicado ao `reference_date` |
| `type_file` | BCB | formato pedido à API |
| `id` | BCB (branch SGS) | código da série no SGS |

Tipos aceitos (`STRING`, `INT`, `DOUBLE`, `TIMESTAMP`, `DATE`) e as divergências entre extractors: ver [../classes/extractors/README.md](../classes/extractors/README.md).

---

## `table_config` — tipagem por listas

Usada pelos extractors que não têm `schema` declarado.

| Chave | Extractor | Efeito |
|---|---|---|
| `date_fields` | MongoDB | troca a sentinela `'0001-01-01'` por `NULL` (não faz cast) |
| `string_fields` | MongoDB | cast para `StringType` |
| `double_fields` | MongoDB | cast para `DoubleType` |
| `binary_fields` | MongoDB | converte para base64 |
| `dataset` | BigQuery | dataset de origem no BigQuery |
| `col_escape_char` | BigQuery | caractere de escape do nome da coluna no `WHERE` |
| `struct_fields` | BigQuery | serializados com `to_json` |
| `integer_fields` | BigQuery | cast para `integer` |
| `float_fields` | BigQuery | cast para `float` |
| `timestamp_fields` | BigQuery | cast para `timestamp` |

No `BigQueryDataExtractor`, **tudo que não estiver em nenhuma dessas listas vira string**.

---

## Chaves presentes nos configs e não lidas por ninguém

Herança de template. Não causam erro, mas também não fazem nada:

| Chave | Onde aparece |
|---|---|
| `parent_columns` | config do GA4 |
| `export_date_included` | config do MongoDB |
| `exclude_columns` | configs do BCB (só Galileo e SharePoint leem) |
| `id` | tabela `eur_ptax` (o branch Olinda não usa `id`) |
| `changed_date` (valor) | configs de BCB e MongoDB — apenas a existência é testada |
| `environment` | `bucket_config_dev` do notebook de Galileo |
