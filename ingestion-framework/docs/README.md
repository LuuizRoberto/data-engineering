# docs/

Documentação do framework.

## Documentos centrais

| Documento | Conteúdo |
|---|---|
| [architecture.md](architecture.md) | As quatro camadas (raw, staging, layer1, layer2), os modos de extração, o ciclo de vida de uma execução, a normalização de nomes de coluna e o registro de execuções |
| [table-config-reference.md](table-config-reference.md) | Todas as chaves de config, o bloco `sec` exigido por cada um dos 23 serviços, e **quais extractors realmente leem cada chave** |
| [known-issues.md](known-issues.md) | Bugs, falhas silenciosas, fragilidades de contrato com a origem, e a lista do que **parece** erro mas é intencional |

## Pipelines documentados

Um documento por pipeline: o que ingere, de onde, com que frequência, e o que é específico dele.

| Documento | Fonte | Tabelas |
|---|---|---|
| [pipelines/bcb-olinda.md](pipelines/bcb-olinda.md) | API Olinda do Banco Central | `eur_ptax` |
| [pipelines/bcb-opendata.md](pipelines/bcb-opendata.md) | API SGS do Banco Central | 5 séries econômicas |
| [pipelines/bigquery-ga4.md](pipelines/bigquery-ga4.md) | Google Analytics 4 via BigQuery | `events`, `user` |
| [pipelines/galileo-files.md](pipelines/galileo-files.md) | Arquivos PGP do processador Galileo | 9 arquivos diários |
| [pipelines/mongodb-myprofit.md](pipelines/mongodb-myprofit.md) | MongoDB Atlas — MyProfit | `CustomerAccount` |
| [pipelines/sharepoint-itau.md](pipelines/sharepoint-itau.md) | SharePoint Online — arquivos do Itaú | 3 arquivos |

## Por onde começar

- Para **entender o framework**: [architecture.md](architecture.md), depois [../classes/README.md](../classes/README.md).
- Para **escrever código novo** no padrão: [../CLAUDE.md](../CLAUDE.md).
- Para **escrever um config**: [table-config-reference.md](table-config-reference.md).
- Para **investigar um comportamento estranho**: [known-issues.md](known-issues.md) — em particular a seção 9, que lista o que não deve ser mexido.
