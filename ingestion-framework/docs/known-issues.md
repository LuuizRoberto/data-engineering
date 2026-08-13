# Problemas conhecidos e armadilhas

Catálogo do que foi observado ao ler o código. **Nada disto foi corrigido** — o repositório preserva o código como está em produção. O propósito da lista é duplo: evitar que alguém (ou uma IA) replique um defeito achando que é padrão, e evitar que alguém "conserte" o que é intencional.

Nenhum item foi verificado em execução; são conclusões da leitura do código.

---

## 1. Falhas silenciosas — os mais graves

### `run()` nunca levanta exceção

`IngestionPipeline.run()` captura toda exceção por tabela, registra `'failed'` em `run_results` e imprime o status final. **Não propaga nada.** Um job Databricks que só chama `run()` termina com sucesso mesmo que todas as tabelas tenham quebrado.

O único notebook que trata isso é o de SharePoint, que checa `run_results` e levanta explicitamente. Replicar esse padrão é o que evita o problema.

### Decriptação do Galileo não é verificada

```python
data = gpg.decrypt(encrypted_data, passphrase = self.sm_passphrase)
decrypted_data = data.data
```

`data.ok` nunca é consultado. Uma decriptação que falha produz conteúdo vazio, que gera um arquivo vazio, um DataFrame vazio, e um snapshot vazio gravado com `result: success`. No modo `update`, esse snapshot vazio vira a base do anti-join do dia seguinte.

### Factory devolve `None` para serviço desconhecido

Não há `else` nem `raise`. `service` com erro de digitação — ou `service: 'ftp'` com `instance` fora de `apex`/`thales`/`marketingcloud` — produz `None`, e a falha aparece depois como `AttributeError: 'NoneType' object has no attribute 'extract'` dentro da thread, com traceback que não aponta para a causa.

### Tipos não reconhecidos viram STRING sem aviso

As cadeias de `if/elif` que traduzem tipo do config para tipo Spark terminam em `else: StringType()`. Efeito real nos configs deste repositório:

- **Galileo** declara `FLOAT` em `authorizedtransactions` e `postedtransactions` e `NUMBER` em `expired_auth`, `dispute_chargeback_report` e `fraud_rule_detail`. Nenhum dos dois está mapeado — todos os valores monetários (`TRANSACTION AMOUNT`, `SETTLE AMOUNT`, `BILLING AMOUNT`, `AUTH_AMT`, `DISPUTE_AMOUNT`, …) chegam ao lake como texto.
- **BigQuery** converte para string tudo que não estiver nas listas de tipos. No config do GA4, `event_timestamp` vira string — e ele compõe o `primary_key` da deduplicação do layer2.

### `BooeleanType` — erro de digitação

Em `bcb_data_extractor.py`, o branch `BOOLEAN` chama `BooeleanType()`. Como `imports` usa `from pyspark.sql.types import *`, o nome só falha em execução. Nenhum config atual usa `BOOLEAN`, então o caminho está dormente.

---

## 2. Bugs de escopo de variável

### `table` em vez de `ftable` no tratamento de erro

Nos dois pipelines, dentro do `except` do laço de futures:

```python
"table_config" : json.dumps(self.ingestion_config['tables'][table])
```

`table` vazou do `for` anterior e vale a **última** tabela da lista. Todas as outras chaves do registro usam `ftable` corretamente. Resultado: toda falha grava o `table_config` da tabela errada em `pipeline_result`. No `IngestionLatestSnapshotPipeline` a mensagem de log também usa `table`.

### `parquet_path` não existe

Em `IngestionLatestSnapshotPipeline.snapshot_table`, o `except` da criação da tabela no BigQuery referencia `parquet_path`, mas a variável se chama `datalake_layer2_parquet_path`. Se aquele bloco disparar, o próprio handler estoura `NameError` e mascara o erro original. Na prática é caminho quase morto, porque o método interno já engole as exceções.

### `uuid` sombreado

`for uuid in self.run_results:` em `persist_pipeline_run_result` sobrescreve o módulo `uuid` dentro do método. Funciona porque o módulo não é usado ali, mas é armadilha para quem editar depois.

### `self.extracted_df` escrito de dentro das threads

`ingest_table` atribui `self.extracted_df` em paralelo, uma vez por tabela. A última a terminar vence. Parece resíduo de depuração.

---

## 3. `extract_mode` e `primary_key`

### Valor de `extract_mode` fora de `update`/`append`

`save_dataframe_to_storage` só atribui `df_lake` nesses dois casos. Qualquer outro valor causa `NameError: df_lake` dentro do `try`, retornado como `'failed'` com mensagem obscura.

### `primary_key` nulo no modo `update`

`IngestionLatestSnapshotPipeline` faz `primary_key.split(',')`. Com `primary_key: None` e `latest_snapshot_mode: 'update'` (serviço diferente de `datastore`), o resultado é `AttributeError`. Só é seguro com modo `full`.

O código também é inconsistente consigo: no caminho de fallback ele lê `table_config['primary_key']` direto, sem o teste `'primary_key' in table_config` que usa poucas linhas acima — `KeyError` se a chave não existir.

### Empates na deduplicação

O `row_number` ordena por `dl_export_datetime` desc. Duas linhas com o mesmo timestamp têm desempate não determinístico — a linha escolhida pode mudar entre execuções.

---

## 4. Detecção de mudança

### Colisão de hash com nulos

```python
sqlfunc.sha2(sqlfunc.concat_ws(*([""] + [col(c).cast("string") for c in df.columns])), 256)
```

O separador é string vazia e `concat_ws` **ignora nulos**. Então `("ab", NULL)` e `("a", "b")` produzem o mesmo hash. No modo `update`, uma mudança que só desloque conteúdo entre colunas dessa forma não é detectada.

### Dependência entre dias, não declarada

O modo `update` lê `staging/dl_export_date=<reference_date>`, isto é, o snapshot de ontem. Se ontem não rodou, o caminho não existe e a tabela falha. Não há verificação nem mensagem que explique a causa. É a razão de reprocessamentos precisarem percorrer as datas em ordem crescente.

---

## 5. Suspeitas que precisam de verificação no ambiente

### O filtro incremental do BigQuery pode nunca ter sido aplicado

Duas evidências, ambas no `BigQueryDataExtractor`:

1. A opção passada é `.option('sqlQuery', query)`. A opção de leitura do conector spark-bigquery é **`query`**; `sqlQuery` não é reconhecida e opções desconhecidas tendem a ser ignoradas.
2. A query filtrada é montada **sem crases** em `{project_id}.{database}.{table}`. Com um project id contendo hífen — `data-prod-295413` — isso é sintaticamente inválido no BigQuery. Se a query estivesse sendo executada, o modo incremental falharia sempre.

A leitura recairia sobre `.option("table", …)`, trazendo a tabela inteira. Confirmar olhando volume e custo real das queries. Vale para o branch genérico; o de GA4 já lê a tabela diária inteira por definição.

### `table` vs `endpoint` divergentes

No branch de carga total do `BigQueryDataExtractor`, a query usa `table_name = table` (a chave do dicionário), enquanto o branch incremental e o `.option("table", …)` usam `endpoint`. No config do GA4 a tabela `user` tem `endpoint: 'pseudonymous_users'` — se aquele branch fosse atingido, apontaria para uma tabela inexistente.

### `col_escape_char: None`

Se um config de BigQuery tiver `changed_date` preenchido e `col_escape_char: None`, a string literal `None` é interpolada em volta do nome da coluna.

---

## 6. Fragilidade de contrato com a origem

### Galileo — schema posicional

O CSV é lido com schema explícito. A ordem das colunas do arquivo precisa bater **exatamente** com a ordem do `schema`. Uma coluna inserida no meio pelo fornecedor desloca tudo: os dados vão para as colunas erradas, com os tipos errados, sem erro.

### SharePoint — validação por conjunto, não por ordem

A validação de nomes compara pertinência nos dois sentidos, mas não a ordem. Um arquivo com as mesmas colunas em ordem trocada passa na validação e depois é lido com schema posicional. É a única brecha de uma validação que, no resto, é a melhor do framework.

### MongoDB — colunas descobertas em um único documento

```python
columns = explode.select(to_json("AccountMoviment").alias('fields')).limit(1)
```

O primeiro documento define o conjunto de campos para todos os registros. Campo presente só em documentos posteriores é descartado silenciosamente — justamente o cenário de schema heterogêneo que caracteriza o MongoDB.

### `replace_header` e `has_header`

O `replace_header` do Galileo troca a primeira linha pelos nomes do `schema`. Se o arquivo vier **sem** cabeçalho, a primeira linha de dados é destruída. No SharePoint, o branch `has_header == False` só trata CSV; em XLSX sem cabeçalho, a primeira linha de dados é consumida como header.

---

## 7. Performance e custo

| Item | Efeito |
|---|---|
| `ThreadPoolExecutor()` sem `max_workers` | paralelismo contra a origem é o default do Python, não uma decisão |
| Layer2 lê o layer1 inteiro | todas as partições, com `mergeSchema`, mesmo para pegar só o mais recente |
| Filtro do MongoDB aplicado depois do `.load()` | sem pushdown: a coleção inteira é varrida a cada execução |
| `pip list` no `imports` | roda duas vezes por execução de notebook |
| Arquivo do SharePoint baixado duas vezes | uma na guarda do notebook, outra no `extract` |
| `copy_to_dbfs` lê o arquivo inteiro em memória | limitação do `dbutils.fs.put` |

---

## 8. Segurança e dados sensíveis

| Item | Observação |
|---|---|
| Chave privada GPG em texto claro | `/tmp/private_key_galileo.asc`, path fixo, nunca removido, compartilhado entre threads |
| Versões de secret fixadas | `versions/1`, `/2`, `/3` conforme o extractor — rotacionar no Secret Manager não tem efeito |
| Senha do MongoDB sem URL-encode | interpolada direta na URI; caractere especial quebra a conexão |
| `eval(row.fields)` no MongoDB | executa conteúdo vindo do banco; além do risco, quebra com `true`/`false`/`null` (um `json.loads` seria equivalente e seguro) |
| PII/PCI sem mascaramento | Galileo ingere `SSN`, `CARD NUMBER`, `SEMI ENCRYPTED PAN`, `DATE OF BIRTH`, endereço e telefone; SharePoint ingere CPF, e-mail, telefone e celular. `exclude_columns` está vazio em todas as tabelas |
| Project id hardcoded | `data-prod-295413` em clients de Storage, BigQuery e nos paths do Secret Manager |

Não são bugs — são decisões de arquitetura. Estão aqui porque determinam quem pode ler os datasets resultantes.

---

## 9. Comportamentos que parecem erro e não são

Não "corrija" nada disto:

| Aparência | Realidade |
|---|---|
| `dir='/'` em `NamedTemporaryFile` | faz o nome sair como `/tmpXXXX`, que concatenado com `dbfs_folder` vira `/tmp/tmpXXXX`. É o mecanismo |
| Fechar o arquivo temporário logo após criá-lo | só o **nome** é aproveitado; quem chama reabre o caminho |
| `'removetable'` no cálculo de `data_origin` | reaproveita `get_parquet_path` sem a parte da tabela |
| `self.timed_ingest_table(self.ingest_table)` avaliado antes do `submit` | devolve o wrapper, que é o que vai para o executor. Está correto |
| Escrever em parquet e reler o mesmo caminho | materializa o resultado e corta a linhagem do Spark |
| `print` em vez de `logging` | convenção do framework |
| Unity Catalog todo comentado | migração iniciada e não concluída, preservada de propósito |

---

## 10. Cosméticos

- `self.sm_client` do `IngestionPipeline`, `self.bq_client` do `BigQueryDataExtractor` e `ds_type` do layer2: criados e nunca usados.
- Typos consistentes (funcionam, mas confundem): `self._servive`, `_projetct_id` (parâmetro recebido e descartado — o project id entra hardcoded), `incial` como placeholder no endpoint do Olinda, `'Pipeline execution succesfull'`.
- `f.close()` depois do `return` em `detect_file_encoding`: código morto.
- `del csv_reader` no SharePoint: o handle já foi fechado pelo `with`.
- `chardet.detect` pode devolver `None` em arquivo vazio — `orig_encoding.lower()` estouraria.
- `BATCH schema` no config do Galileo parece corrupção de copiar/colar; como o schema é posicional, o alinhamento continua correto mas o nome da coluna está errado.
- No SGS, a URL final fica com `formato` duplicado (uma vez no `endpoint`, outra em `params`).
- O arquivo raw do BCB é salvo com extensão `.csv` mas contém JSON Lines.
- Exceções por nome de tabela hardcoded: `if table == 'eur_ptax'` no BCB, `if table == "search_comments"` no SharePoint. Nos dois casos o `type` do config já carregava a informação necessária para distinguir.
