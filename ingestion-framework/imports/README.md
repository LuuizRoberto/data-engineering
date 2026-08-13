# imports/

Notebook único com **todos** os imports do framework. É sempre a primeira célula de qualquer notebook de execução:

```python
%run ../../imports
```

Como `%run` no Databricks executa o notebook no mesmo escopo, todos os nomes importados aqui ficam disponíveis para as classes e para os notebooks. É por isso que nenhum arquivo em `classes/` tem `import` no topo.

## O que revela sobre o framework

As dependências aqui são de todos os 23 extractors, não só dos 5 presentes neste repositório. Elas indicam as fontes cobertas pelo framework completo:

| Import | Fonte correspondente |
|---|---|
| `boto3`, `botocore` | S3, Cognito |
| `pysftp` | FTP (Apex, Thales, Marketing Cloud) |
| `gnupg` | Galileo (arquivos PGP) |
| `openpyxl` | SharePoint (XLSX) |
| `chardet` | SharePoint (detecção de encoding) |
| `zipfile` | FTP Apex (arquivos compactados com senha) |
| `simple_salesforce` | Salesforce |
| `office365` | SharePoint |
| `facebook_business` | Facebook Ads |
| `google.cloud.datastore`, `Entity`, `DatetimeWithNanoseconds` | Datastore |
| `requests` | BCB, Nasdaq, Gupy, AppsFlyer, HiPlatform, MyProfit, Apex, FED Funds |

## Pontos de atenção

- **`spark` e `dbutils` não aparecem aqui** e nem podem: são globais injetados pelo Databricks. É a razão de o framework não rodar fora da plataforma.
- `from pyspark.sql.types import *` é um import curinga. É por isso que um nome de tipo digitado errado (como o `BooeleanType` do extractor do BCB) não é detectado na leitura e só estoura em execução.
- Os dois blocos de verificação no fim rodam `subprocess.check_output("pip list")`, o que custa alguns segundos **a cada execução de notebook**.
- O guard do `simple_salesforce` não funciona: existe um `import simple_salesforce` incondicional algumas linhas acima do `if`. Se o pacote não estiver instalado, o notebook quebra antes de chegar na verificação. O bloco do `facebook_business` está correto, porque não tem import solto antes.
- Os comentários dizem "realiza a instalação do pacote", mas nenhum dos dois blocos instala nada — apenas importa.

Nada disso foi alterado: o arquivo está como no original.
