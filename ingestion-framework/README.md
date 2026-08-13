# Ingestion Framework

Framework de ingestão de dados orientado a objetos, escrito em Python/PySpark e executado em **notebooks Databricks**, com destino em **Google Cloud Storage** e **BigQuery**.

Este repositório é um **arquivo histórico**: o código está preservado exatamente como foi escrito em produção, sem nenhuma alteração. O objetivo é servir de referência de estrutura e estilo — inclusive para orientar assistentes de IA a escrever código novo seguindo o mesmo padrão. Ver [CLAUDE.md](CLAUDE.md).

---

## O que o framework faz

Ele ingere dados de fontes heterogêneas (APIs, bancos, arquivos, SaaS) e os deposita em um data lake em três camadas, criando automaticamente as tabelas externas correspondentes no BigQuery.

```
                  ┌─────────────────┐
   fonte externa  │  DataExtractor  │  extract()
                  └────────┬────────┘
                           │ DataFrame + arquivo temporário
                           ▼
   ┌──────────────────────────────────────────────────┐
   │              IngestionPipeline                   │
   │                                                  │
   │  raw      arquivo original, byte a byte          │  só fontes de arquivo/API
   │   │       gs://<raw>/…/dl_export_date=YYYY-MM-DD │
   │   ▼                                              │
   │  staging  snapshot completo do dia, em parquet   │  sempre
   │   │       + dl_hash_key + dl_export_datetime     │
   │   ▼                                              │
   │  layer1   delta (update) ou cópia (append)       │  sempre
   └──────────────────────┬───────────────────────────┘
                          ▼
   ┌──────────────────────────────────────────────────┐
   │        IngestionLatestSnapshotPipeline           │
   │  layer2   estado atual, deduplicado por          │
   │           primary_key, ou cópia integral (full)  │
   └──────────────────────────────────────────────────┘
```

Cada camada gera uma tabela externa no BigQuery, em datasets nomeados a partir do bucket.

---

## Estrutura do repositório

| Pasta | Conteúdo | README |
|---|---|---|
| `classes/` | Classes base, pipelines e a Factory | [classes/README.md](classes/README.md) |
| `classes/extractors/` | Extractors concretos, um por fonte | [classes/extractors/README.md](classes/extractors/README.md) |
| `imports/` | Notebook único de imports, carregado por todos | [imports/README.md](imports/README.md) |
| `ingestion-conf/` | Configs declarativos, um por fonte/database | [ingestion-conf/README.md](ingestion-conf/README.md) |
| `ingestion/` | Notebooks de execução (orquestração) | [ingestion/README.md](ingestion/README.md) |
| `docs/` | Arquitetura, referência de config, pipelines, problemas conhecidos | [docs/README.md](docs/README.md) |

Documentos centrais:

- [docs/architecture.md](docs/architecture.md) — as três camadas, os modos de extração, o ciclo de vida de uma execução
- [docs/table-config-reference.md](docs/table-config-reference.md) — todas as chaves de config e **quais extractors realmente leem cada uma**
- [docs/known-issues.md](docs/known-issues.md) — bugs, armadilhas e comportamentos silenciosos observados no código
- [docs/pipelines/](docs/pipelines/) — uma página por pipeline documentado

---

## Como uma execução funciona

Um notebook em `ingestion/` faz sempre a mesma coisa:

```python
%run ../../imports
%run ../../classes/classes
%run ../../ingestion-conf/conf_ingestion_<...>

bucket_config_prod = { 'datalake_bucket': …, 'staging_bucket': …, 'raw_bucket': …, 'databricks_bucket': … }

export_date    = pydt.datetime.today()
reference_date = export_date - pydt.timedelta(days=1)

pipeline = IngestionPipeline(config, bucket_config_prod)
pipeline.run(export_date, reference_date)
```

O `IngestionPipeline` lê `config['service']`, pede o extractor à `DataExtractorFactory`, e roda **uma thread por tabela** do config.

### As duas datas

Esta é a convenção mais importante do framework:

- **`export_date`** — a data da execução. Define a partição escrita (`dl_export_date=YYYY-MM-DD`) em raw, staging e layer1.
- **`reference_date`** — sempre `export_date - 1 dia` nos notebooks. Tem três usos, dependendo do contexto:
  1. no `IngestionPipeline`, modo `update`: aponta para o snapshot de **staging do dia anterior**, contra o qual é feito o anti-join por `dl_hash_key`;
  2. nos extractors de janela (BCB, MongoDB): é a **data final** do período consultado na origem;
  3. no extractor de BigQuery/GA4: é o **sufixo da tabela diária** (`events_YYYYMMDD`) e, no SharePoint, a data no **nome do arquivo**.

**Consequência operacional:** no modo `update`, se a execução do dia anterior não rodou, o caminho de staging de `reference_date` não existe e a tabela falha. Existe uma dependência dia-a-dia implícita — é por isso que reprocessamento se faz com um laço de datas em ordem crescente (ver `ingestion/bigquery/`).

---

## Ambiente e restrições

- **Só executa dentro do Databricks.** `spark` e `dbutils` são globais injetados pela plataforma e nunca são importados. Nenhuma classe é instanciável ou testável fora de um notebook.
- **Projeto GCP `data-prod-295413` está fixo no código**, em vários pontos (clients de Storage, BigQuery e os paths do Secret Manager).
- Credenciais vêm do **Secret Manager**, com a versão do secret fixada no código (`versions/1`, `versions/2`, `versions/3` conforme o extractor). Rotacionar um secret não tem efeito até o código ser alterado.
- Buckets são passados por `bucket_config` no notebook, com variantes `prod` e `dev-`.

---

## Estado de completude deste arquivo

O framework original tem **23 extractors** declarados na `DataExtractorFactory`. Este repositório contém a implementação de **5**:

| Implementados | Declarados na Factory, não incluídos |
|---|---|
| `BCBDataExtractor` | `S3`, `Jira`, `Salesforce`, `CloudSQL`, `HiChat`, `FacebookAds`, `Gcs`, `GcsMultFiles`, |
| `GalileoDataExtractor` | `MyProfit`, `Nasdaq`, `ApexAPI`, `Datastore`, `FtpApex`, `FtpThales`, `Gupy`, |
| `MongoDBDataExtractor` | `Cognito`, `FEDFunds`, `AppsFlyer`, `MarketingCloudFTP` |
| `BigQueryDataExtractor` | (comentados na Factory: `Worthix`, `QultureRocks`) |
| `SharepointDataExtractor` | |

A `DataExtractorFactory` está incluída na íntegra, então o contrato de construção de todos os 23 está documentado mesmo sem o código.

Também não estão incluídos: o notebook `layer1_classes`, o segundo config de SharePoint (o da tabela `search_comments`) e notebooks de `IngestionLatestSnapshotPipeline` avulsos.
