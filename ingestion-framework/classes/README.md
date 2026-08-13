# classes/

Classes base, pipelines e a fábrica de extractors.

No workspace original, tudo isto vive em **um único notebook**, `classes/classes`, carregado pelos notebooks de execução com `%run ../../classes/classes`. Aqui cada classe foi separada em um arquivo para leitura, mas o **código é literal** — nenhuma linha foi alterada, nada foi adicionado.

Por isso os arquivos **não são módulos Python importáveis**: não há `import` nenhum neles. As dependências (`ABC`, `storage`, `pydt`, `sqlfunc`, `spark`, …) vêm do notebook `imports/` e dos globais do Databricks.

## Arquivos

| Arquivo | Classe | Papel |
|---|---|---|
| `pipeline.py` | `Pipeline(ABC)` | Base de todos os pipelines. Cria o client de GCS e expõe `upload_blob` e `strip_accents`. Declara `run()` abstrato. |
| `data_extractor.py` | `DataExtractor(ABC)` | Base de todos os extractors. Declara `extract()` abstrato e expõe os utilitários de arquivo temporário e DBFS. |
| `ingestion_pipeline.py` | `IngestionPipeline(Pipeline)` | Orquestra raw → staging → layer1 e cria as tabelas externas no BigQuery. |
| `ingestion_latest_snapshot_pipeline.py` | `IngestionLatestSnapshotPipeline(Pipeline)` | Gera a camada layer2 a partir do layer1. |
| `data_extractor_factory.py` | `DataExtractorFactory` | Mapeia `config['service']` para a classe de extractor, lendo os parâmetros de `config['sec']`. |
| `extractors/` | — | Os extractors concretos. Ver [extractors/README.md](extractors/README.md). |

## Hierarquia

```
Pipeline (ABC)
├── IngestionPipeline                     camadas raw / staging / layer1
└── IngestionLatestSnapshotPipeline       camada layer2

DataExtractor (ABC)
├── BCBDataExtractor
├── GalileoDataExtractor
├── MongoDBDataExtractor
├── BigQueryDataExtractor
└── SharepointDataExtractor
```

Repare que `IngestionLatestSnapshotPipeline` **não** herda de `IngestionPipeline`: as duas estendem `Pipeline` diretamente e reimplementam `get_parquet_path`, o decorador de cronometragem, `persist_pipeline_run_result` e `create_big_query_external_table`.

## Uma diferença que importa

As duas classes têm um método chamado `create_big_query_external_table`, e eles fazem **coisas opostas**:

| | `IngestionPipeline` | `IngestionLatestSnapshotPipeline` |
|---|---|---|
| Tabela já existe | mantém e adiciona apenas colunas novas (patch aditivo) | `delete_table` + `create_table` |
| Efeito colateral | nenhum | permissões e metadados da tabela são perdidos a cada execução |
| Particionamento Hive | sempre, via parâmetros | só se o dataset for `avenue_databricks` ou `dev_avenue_databricks` |

Nos dois casos a evolução de schema é **apenas aditiva**: mudança de tipo ou remoção de coluna não é detectada.

## Contratos entre as classes

**`DataExtractor.extract` devolve uma tupla de 3:**

```python
return df, dbfs_file_path, original_source_name
```

Quando os dois últimos são `None`, o `IngestionPipeline` pula o upload para a camada raw. É assim que extractors de banco (MongoDB, BigQuery) se distinguem dos de arquivo/API (BCB, Galileo, SharePoint).

**Métodos opcionais de reprocessamento**, chamados conforme as flags de `run()`:

| Flag | Método chamado | Implementado em |
|---|---|---|
| `from_raw_file=True` | `extract_from_raw(raw_path, table_config)` | apenas `SharepointDataExtractor` |
| `from_staging=True` | `extract_from_staging(staging_path)` | nenhum extractor deste repositório |

Chamar `run(..., from_staging=True)` com qualquer um dos extractors aqui presentes resulta em `AttributeError`.

## Detalhes propositais

- `data_origin` é calculado passando a string literal `'removetable'` como nome de tabela para `get_parquet_path` e removendo-a em seguida. É um jeito de reaproveitar o método sem a parte final do caminho.
- O `dl_hash_key` é o SHA-256 de todas as colunas concatenadas — é ele que define, no modo `update`, se uma linha mudou.
- `dl_export_datetime` é o timestamp da execução e é a coluna de ordenação da deduplicação do layer2.
- O bloco de Unity Catalog (`create_unity_catalog_table` e sua chamada) está inteiramente comentado: migração iniciada e não concluída. Preservado como estava.

Comportamentos que **são** defeitos estão em [../docs/known-issues.md](../docs/known-issues.md).
