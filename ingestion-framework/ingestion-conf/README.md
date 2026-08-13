# ingestion-conf/

Configs declarativos. Cada arquivo define uma única variável global chamada `config` e é carregado pelo notebook de execução com `%run`:

```python
%run ../../ingestion-conf/conf_ingestion_avenue_sharepoint_teamdata_itau
```

Como todos declaram a mesma variável, **um config por notebook** — carregar dois no mesmo escopo faz o segundo sobrescrever o primeiro.

## Arquivos

| Arquivo | Service | Tabelas | Notebook |
|---|---|---|---|
| `conf_ingestion_avenue_bcb_api_olinda.py` | `bcb` / olinda | 1 (`eur_ptax`) | `ingestion/bcb/` |
| `conf_ingestion_avenue_bcb_api_opendata.py` | `bcb` / opendata | 5 séries SGS | — |
| `conf_ingestion_avenue_bigquery_table.py` | `bigquery` / googleanalytics | 2 (`events`, `user`) | `ingestion/bigquery/` |
| `conf_ingestion_avenue_galileo_files.py` | `galileo` | 9 arquivos diários | `ingestion/galileo/` |
| `conf_ingestion_evp_myprofit_prd_mongo_myprofit.py` | `mongodb` | 1 (`CustomerAccount`) | `ingestion/mongodb/` |
| `conf_ingestion_avenue_sharepoint_teamdata_itau.py` | `sharepoint` | 3 arquivos do Itaú | `ingestion/sharepoint/` |

## Anatomia

```python
config = {
    "project_id": "avenue",       # ─┐
    "service"   : "sharepoint",   #  │ compõem o caminho no GCS
    "instance"  : "teamdata",     #  │ e escolhem o extractor (service)
    "database"  : "itau",         # ─┘
    "type"      : "csv",          # subtipo; alguns extractors usam, outros ignoram
    "sec"       : { … },          # bloco de conexão/credencial
    "tables"    : { … }           # uma entrada por tabela
}
```

Caminho resultante no bucket, com `-`, `.` e `/` trocados por `_` em cada componente:

```
<prefix><service>/<project_id>/<instance>/<database>/<table>
```

`prefix` é vazio no staging, `layer1/` no datalake e `layer2/ingestion_latest_snapshot/` no snapshot.

## O bloco `sec`

É um bloco livre, interpretado por cada extractor de um jeito. A mesma chave pode significar coisas diferentes:

| Config | `sec.host` significa |
|---|---|
| BCB Olinda | URL base da API, com o caminho do serviço OData |
| BCB OpenData | URL base da API |
| BigQuery | o **project id** do GCP |

A `DataExtractorFactory` é quem define quais chaves de `sec` cada serviço exige. A tabela completa está em [../docs/table-config-reference.md](../docs/table-config-reference.md).

## Regras que valem para todo config

**Nomes de coluna.** `schema` e `header` usam os nomes **da origem** (com espaços e pontuação); `primary_key` usa os nomes **já normalizados** pelo pipeline. Exemplo real: o campo `TRANSACTION CODE/TYPE` do Galileo aparece no `primary_key` como `TRANSACTION_CODETYPE` — a barra é removida, não virou underscore. Detalhes da regra no [CLAUDE.md](../CLAUDE.md).

**`schema` vs `header`.** São a mesma estrutura (`[{field, type}]`) com nomes diferentes: SharePoint usa `header`, Galileo e BCB usam `schema`. Não são intercambiáveis — cada extractor lê apenas a sua.

**Chaves inertes.** Vários configs trazem chaves que o extractor correspondente não lê. Não são erro, são herança de template — mas quem escreve config novo precisa saber quais realmente têm efeito. Exemplos: `exclude_columns` nos configs de BCB (só o Galileo e o SharePoint leem), `parent_columns` no de GA4 (nenhum extractor deste repositório lê), `id` na tabela `eur_ptax` (aquele branch não usa `id`).

**Chaves de nível de config lidas pelo notebook, não pelas classes.** `modification_date` (GA4) alimenta o laço de reprocessamento; `export_date_included` (MongoDB) não é lido por nenhum código deste repositório.

**Modos.** `extract_mode` (`update` ou `append`) governa o layer1; `latest_snapshot_mode` (`update`, `append` ou `full`) governa o layer2 e, quando presente, **sobrepõe** o `extract_mode`. Ver [../docs/architecture.md](../docs/architecture.md).
