# Pipeline — MongoDB MyProfit

Movimentações de conta do produto MyProfit, em um cluster MongoDB Atlas.

| | |
|---|---|
| **Notebook** | [`ingestion/mongodb/ingestion_evp_myprofit_prd_mongo_myprofit.py`](../../ingestion/mongodb/ingestion_evp_myprofit_prd_mongo_myprofit.py) |
| **Config** | [`ingestion-conf/conf_ingestion_evp_myprofit_prd_mongo_myprofit.py`](../../ingestion-conf/conf_ingestion_evp_myprofit_prd_mongo_myprofit.py) |
| **Extractor** | `MongoDBDataExtractor` |
| **Origem** | `mongodb+srv://…@evp-myprofit-prd.a4flz.mongodb.net/MyProfitEVP` |
| **Autenticação** | Secret Manager: `user-evp-myprofit-prd-MyProfitEVP` e `pwd-evp-myprofit-prd-MyProfitEVP` (ambos **versions/1**) |
| **Caminho no lake** | `mongodb/evp_myprofit_prd/evp_myprofit_prd/MyProfitEVP/CustomerAccount` |

A URI é montada a partir dos campos do config: `{service}+srv://{user}:{pwd}@{instance}.a4flz.{type}.net/{database}`. O identificador de cluster `a4flz` está hardcoded no extractor.

## Tabela

### `CustomerAccount`

| | |
|---|---|
| Coleção | `CustomerAccount` |
| `extract_mode` | `update` |
| `latest_snapshot_mode` | ausente → usa `update` |
| `primary_key` | `MovimentId` |
| `changed_range` | `7 days` |
| Coluna do filtro | `PositionDate` (hardcoded no extractor) |

**O grão da saída não é a conta, é o movimento.** O extractor faz `posexplode` do array `AccountMoviment` dentro de cada documento — cada linha resultante é uma movimentação. O nome da tabela no lake não reflete isso.

### Tipagem

| Lista no config | Campos | Efeito |
|---|---|---|
| `date_fields` | `PositionDate` | troca `'0001-01-01'` por `NULL` (sem cast) |
| `string_fields` | `AccountNumber` | cast para `StringType` |
| `double_fields` | `PositionValue`, `CashValue`, `Provents`, `Purchases`, `PurchasesCrypto`, `Deposits`, `Sales`, `SalesCrypto`, `Withdrawals` | cast para `DoubleType` |
| `binary_fields` | `MovimentId`, `CustomerId`, `id` | convertidos para **base64** |

O `primary_key` (`MovimentId`) é um campo binário, então a deduplicação do layer2 roda sobre a representação base64.

## Fluxo

1. Lê a coleção inteira com o conector `com.mongodb.spark.sql.DefaultSource`.
2. `posexplode` do array `AccountMoviment`, projetando `CustomerId.data` e `_id.data`.
3. Descobre o conjunto de campos do movimento serializando **o primeiro documento** com `to_json` e avaliando o resultado.
4. Renomeia o `_id` do movimento para `MovimentId`.
5. Aplica a tipagem por listas.
6. Filtra `PositionDate` entre `export_date - 7 dias` e `reference_date`.
7. Retorna `(df, None, None)` — **sem camada raw**.
8. Layer1: anti-join por `dl_hash_key` contra o staging de `reference_date`.

## Pontos de atenção

- **A classe é específica desta coleção.** Apesar do nome genérico `MongoDBDataExtractor`, `CustomerId`, `AccountMoviment` e `PositionDate` estão fixos no código, e o log diz `"Starting AccountMoviment table"` independentemente da tabela. Para ingerir outra coleção, escreva um extractor novo — não tente parametrizar este.
- **Descoberta de schema em um único documento.** O `.limit(1)` faz o primeiro documento definir os campos de todos os registros. Campo presente apenas em documentos posteriores é descartado silenciosamente — exatamente o cenário de schema heterogêneo que caracteriza o MongoDB.
- **`eval` sobre dado do banco.** O conjunto de campos é obtido com `eval(row.fields)` sobre a string JSON. Além do risco de execução de código, quebra com `true`, `false` ou `null` no documento.
- **Sem pushdown.** O filtro de data é aplicado depois do `.load()`: a coleção inteira é varrida a cada execução.
- **`changed_date` é decorativo.** Vale `PositionDate` e coincide com a coluna filtrada, mas o extractor nunca lê a chave — a coluna está hardcoded.
- **Unidade de `changed_range` ignorada.** Sempre `timedelta(days=…)`. `"3 Months"` significaria 3 dias.
- **Senha sem URL-encode** na URI; caractere especial quebraria a conexão.
- `export_date_included: False` no config não é lido por nenhum código deste repositório.
