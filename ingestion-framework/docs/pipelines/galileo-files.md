# Pipeline — Galileo (arquivos PGP)

Arquivos diários do processador de cartões Galileo, entregues criptografados em um bucket do GCS.

| | |
|---|---|
| **Notebook** | [`ingestion/galileo/ingestion_avenue_galileo_files.py`](../../ingestion/galileo/ingestion_avenue_galileo_files.py) |
| **Config** | [`ingestion-conf/conf_ingestion_avenue_galileo_files.py`](../../ingestion-conf/conf_ingestion_avenue_galileo_files.py) |
| **Extractor** | `GalileoDataExtractor` |
| **Origem** | `gs://bkt-trading-prod-av-galileo-skldsvth/<arquivo>_{dateint}.csv.gpg` |
| **Autenticação** | Secret Manager: passphrase (`pwd-file-galileo`, **versions/2**) e chave privada GPG (`pvkey-galileo`, **versions/3**) |
| **Caminho no lake** | `galileo/avenue/file/bkt_trading_prod/<tabela>` |

## Tabelas

Nove arquivos, todos com o mesmo padrão: `extract_mode: append` + `latest_snapshot_mode: update`. O arquivo diário completo é empilhado no layer1 e deduplicado por `primary_key` no layer2.

| Tabela | `primary_key` (nomes já normalizados) |
|---|---|
| `customermasterfile` | `GALILEO_ACCOUNT_ID, UNIQUE_PROGRAM_ID` |
| `accountcard` | `UNIQUE_PROGRAM_ID, CARD_IDENTIFIER` |
| `authorizedtransactions` | `UNIQUE_PROGRAM_ID, AUTHORIZATION_CODE, NETWORK_CODE` |
| `postedtransactions` | `UNIQUE_PROGRAM_ID, AUTHORIZATION_CODE, TRANSACTION_CODETYPE` |
| `lookup` | `FILE_TYPE, FIELD_NAME, CODE` |
| `settlement` | `Basebin, Date, Network` |
| `expired_auth` | `Program_ID, Transaction_ID, Network_Code` |
| `dispute_chargeback_report` | `DISPUTE_ID` |
| `badpan_authorizations` | `AUTHORIZATION_IDENTIFICATION_RESPONSE, SLI_INDICATOR, TRANSACTION_DATETIME` |
| `fraud_rule_detail` | `AUTH_ID, AUTH_TS, MERCH_ID, XID, TRANSACTION_RESULT` |

O nome do arquivo usa `{dateint}` substituído por **`export_date`** no formato `%Y%m%d` — diferente do SharePoint, que usa `reference_date`.

## Fluxo

1. Escreve a chave privada do Secret Manager em `/tmp/private_key_galileo.asc`.
2. Baixa o `.csv.gpg` do bucket do fornecedor para o DBFS.
3. Importa a chave no `gnupg` e decripta usando a passphrase.
4. Se `replace_header: True`, substitui a primeira linha do arquivo pelos nomes do `schema`. Só `postedtransactions` usa.
5. Monta o `StructType` a partir do `schema` e lê o CSV **com schema explícito**.
6. Descarta linhas totalmente nulas e registros de trailer (primeira coluna contendo `TTRAILER`).
7. Aplica `exclude_columns` (vazio em todas as tabelas atuais).
8. Retorna `(df, dbfs_file_path, source_blob_name)` — há camada raw.
9. Layer1: `append`, a partição do dia recebe o arquivo inteiro.
10. Layer2: `update`, `row_number` por `primary_key` ordenado por `dl_export_datetime` desc.

## Onde a regra de normalização aparece

Este config é o melhor exemplo da regra descrita no [CLAUDE.md](../../CLAUDE.md): o `schema` usa os nomes do arquivo, o `primary_key` usa os nomes normalizados.

| No `schema` | No `primary_key` |
|---|---|
| `UNIQUE PROGRAM ID` | `UNIQUE_PROGRAM_ID` |
| `TRANSACTION CODE/TYPE` | `TRANSACTION_CODETYPE` |
| `TRANSACTION DATE/TIME` | `TRANSACTION_DATETIME` |

A barra desaparece; não vira underscore.

## Pontos de atenção

- **Leitura posicional.** O CSV é lido com schema explícito: a ordem das colunas do arquivo precisa bater exatamente com a ordem do `schema`. Uma coluna inserida no meio pelo fornecedor desloca tudo silenciosamente.
- **Tipos `FLOAT` e `NUMBER` viram string.** O mapeamento do extractor cobre `STRING`, `INT`, `DOUBLE`, `TIMESTAMP` e `DATE`. Os configs declaram `FLOAT` em `authorizedtransactions`/`postedtransactions` e `NUMBER` em `expired_auth`, `dispute_chargeback_report` e `fraud_rule_detail` — todos os valores monetários chegam ao lake como texto.
- **Decriptação não verificada.** `data.ok` não é consultado; uma falha produz DataFrame vazio com `result: success`.
- **Chave privada em texto claro** em caminho fixo do DBFS, nunca removida, reescrita por todas as threads.
- **Dado sensível sem mascaramento.** `customermasterfile` traz `SSN`, `DATE OF BIRTH`, endereço, telefone e e-mail; `accountcard` traz `CARD NUMBER`, `SEMI ENCRYPTED PAN` e `LAST 4 PAN`. `exclude_columns` está vazio em todas as tabelas — decisão de arquitetura que determina quem pode ler esses datasets.
- O campo `BATCH schema` no `schema` de `postedtransactions` parece corrupção de copiar/colar. Como a leitura é posicional, o alinhamento continua correto; só o nome da coluna está errado.

## Operação

Execução diária. Para reprocessar um dia específico, o notebook traz a linha comentada:

```python
export_date = pydt.datetime.today()
# export_date = pydt.datetime(2025, 9, 4)
```

Como o modo é `append`, reprocessar o mesmo dia sobrescreve a partição correspondente — não duplica.
