# Pipeline — BCB Olinda (PTAX EUR)

Cotação diária do euro publicada pelo Banco Central na API Olinda.

| | |
|---|---|
| **Notebook** | [`ingestion/bcb/ingestion_avenue_bcb_api_olinda.py`](../../ingestion/bcb/ingestion_avenue_bcb_api_olinda.py) |
| **Config** | [`ingestion-conf/conf_ingestion_avenue_bcb_api_olinda.py`](../../ingestion-conf/conf_ingestion_avenue_bcb_api_olinda.py) |
| **Extractor** | `BCBDataExtractor` |
| **Origem** | `https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/CotacaoMoedaPeriodo(...)` |
| **Autenticação** | nenhuma (API pública) |
| **Caminho no lake** | `bcb/avenue/api/olinda/eur_ptax` |

## Tabela

### `eur_ptax`

Cotações de fechamento do EUR em uma janela móvel.

| | |
|---|---|
| `extract_mode` | `update` |
| `latest_snapshot_mode` | ausente → usa `update` |
| `primary_key` | `dataHoraCotacao` |
| `changed_range` | `7 days` |
| Janela consultada | `export_date - 7 dias` até `reference_date` |
| Formato de data na URL | `MM-DD-YYYY` |

Colunas: `paridadeCompra`, `paridadeVenda`, `cotacaoCompra`, `cotacaoVenda`, `dataHoraCotacao`, `tipoBoletim`.

O filtro OData `tipoBoletim eq 'Fechamento'` está embutido no `endpoint`.

## Fluxo

1. Calcula a janela a partir de `changed_range`. Como a unidade é `days`, usa `timedelta`.
2. Monta a URL com `endpoint.format(incial=…, final=…, format=…)` — o placeholder tem o typo `incial`, que precisa ser mantido no config.
3. `GET` sem parâmetros extras; a resposta vem em `response.json()['value']`.
4. Grava cada registro como uma linha JSON em arquivo temporário no DBFS.
5. `spark.read.json` lê o arquivo — **o `schema` do config não é aplicado**, o Spark infere.
6. Retorna `(df, dbfs_file_path, 'eur_ptax.csv')`, então há camada raw.
7. Layer1: anti-join por `dl_hash_key` contra o staging de `reference_date`.
8. Layer2 (se executado): deduplica por `dataHoraCotacao`.

## Especificidades

- **`eur_ptax` é tratado por nome**, com `if table == 'eur_ptax'` no extractor. É esse `if` que seleciona o formato de data e o caminho da resposta — na prática, distingue a API Olinda da API SGS. O `type` do config (`olinda` vs `opendata`) já carregava essa informação.
- O arquivo salvo na camada raw tem extensão `.csv` mas contém JSON Lines.
- O `schema` declarado no config é inerte: serve como documentação das colunas esperadas, não como contrato aplicado.
- `id` e `exclude_columns` no config são chaves inertes neste branch.

## Operação

Execução diária. Sem retentativa, sem `raise` — uma falha na API deixa o job verde. Ver [../known-issues.md](../known-issues.md), seção 1.

No notebook há uma linha comentada referenciando `run_tables = ['bcb_cdi_daily']`, tabela que não existe neste config. Indica que o config já teve outras tabelas ou que existe uma versão mais recente não incluída neste arquivo.
