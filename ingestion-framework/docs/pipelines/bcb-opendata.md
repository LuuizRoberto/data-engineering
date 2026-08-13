# Pipeline — BCB OpenData (séries SGS)

Séries econômicas do Sistema Gerenciador de Séries Temporais do Banco Central.

| | |
|---|---|
| **Notebook** | não incluído neste arquivo |
| **Config** | [`ingestion-conf/conf_ingestion_avenue_bcb_api_opendata.py`](../../ingestion-conf/conf_ingestion_avenue_bcb_api_opendata.py) |
| **Extractor** | `BCBDataExtractor` |
| **Origem** | `https://api.bcb.gov.br/dados/serie/bcdata.sgs.{id}/dados` |
| **Autenticação** | nenhuma (API pública) |
| **Caminho no lake** | `bcb/avenue/api/opendata/<tabela>` |

## Tabelas

Todas com o mesmo formato de saída — duas colunas, `data` (DATE) e `valor` (DOUBLE) — e a mesma configuração, variando apenas o código da série e a janela.

| Tabela | Série SGS | `changed_range` | Frequência da série |
|---|---|---|---|
| `selic_daily` | 11 | `7 days` | diária |
| `selic_monthly` | 4390 | `12 Months` | mensal |
| `igpm_monthly` | 189 | `12 Months` | mensal |
| `inpc_monthly` | 188 | `12 Months` | mensal |
| `ipca_monthly` | 433 | `12 Months` | mensal |

Todas: `extract_mode: update`, `primary_key: data`, `changed_date: data`, sem `latest_snapshot_mode` (logo, o layer2 deduplica por `data`).

## Fluxo

1. Calcula a janela a partir de `changed_range`. As séries mensais usam `'12 Months'`, com **M maiúsculo** — é essa capitalização exata que aciona `relativedelta(months=…)` no extractor. Escrito de outro jeito, cairia no `else` e viraria "ontem".
2. Monta a URL com `endpoint.format(id=…, type_file=…)`.
3. `GET` com `params` contendo `formato`, `dataInicial` e `dataFinal` no formato `DD/MM/YYYY`.
4. Grava JSON Lines em arquivo temporário, lê com `spark.read.json` e converte `data` com `to_date(col("data"), "dd/MM/yyyy")`.
5. Retorna `(df, dbfs_file_path, '<tabela>.csv')` — há camada raw.
6. Layer1: anti-join por `dl_hash_key` contra o staging de `reference_date`.

As cinco tabelas rodam em paralelo, uma thread cada.

## Especificidades

- Este é o único pipeline em que a unidade de `changed_range` realmente importa, e o BCB é o único extractor que a interpreta. O mesmo `'12 Months'` num config de MongoDB ou BigQuery significaria **12 dias**.
- A janela de 12 meses para séries mensais dá margem para revisões retroativas dos índices: o BCB republica valores, e o anti-join por hash captura a alteração.
- A URL final fica com `formato` duplicado — uma vez no `endpoint`, outra em `params`. Inofensivo.
- Como no Olinda, o `schema` do config não é aplicado: `spark.read.json` infere.
- Um `GET` que retorne erro HTTP não é verificado (`raise_for_status` ausente) e falha adiante no `.json()`.
