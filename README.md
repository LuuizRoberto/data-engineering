# data-engineering

Repositório pessoal de projetos e referências de engenharia de dados.

## Projetos

### [ingestion-framework/](ingestion-framework/)

Framework de ingestão de dados orientado a objetos — Python/PySpark sobre Databricks, com destino em Google Cloud Storage e BigQuery. Arquitetura em quatro camadas (raw → staging → layer1 → layer2), configuração declarativa por fonte e extractors plugáveis via factory.

É um **arquivo histórico de código de produção**: o código está preservado exatamente como foi escrito, sem nenhuma alteração. Serve como referência de estrutura e estilo — inclusive para orientar assistentes de IA a escrever código novo no mesmo padrão (ver [`ingestion-framework/CLAUDE.md`](ingestion-framework/CLAUDE.md)).

Documentação: [arquitetura](ingestion-framework/docs/architecture.md) · [referência de config](ingestion-framework/docs/table-config-reference.md) · [problemas conhecidos](ingestion-framework/docs/known-issues.md) · [pipelines](ingestion-framework/docs/pipelines/)

### [simple-ingestion/](simple-ingestion/)

Scripts de ingestão avulsos — um arquivo, procedural, sem as camadas e a orquestração do framework. Para fontes únicas e volume pequeno, onde a estrutura completa não se paga.

Hoje: ingestão das issues de contas a pagar do Jira, com resolução de objetos do Assets/CMDB.

## Convenção do repositório

Todo código aqui é **arquivo histórico e está preservado verbatim** — nada é alterado, refatorado ou corrigido. Problemas identificados na leitura ficam documentados nos READMEs e em `docs/known-issues.md`, nunca no código.
