# ingestion/

Notebooks de execução — a camada de orquestração do framework. Cada notebook representa **um pipeline agendado** no Databricks.

> **Transcrição.** Os arquivos desta pasta foram transcritos a partir de capturas de tela dos notebooks originais, célula a célula. O código é fiel ao que estava visível; onde o screenshot cortou conteúdo, há um comentário explícito `# [... não visível no screenshot de origem]`. As demais pastas do repositório vêm do código-fonte em texto e são literais.
>
> Os separadores `# COMMAND ----------` marcam a divisão de células, no formato de exportação de notebooks do Databricks.

## Arquivos

| Notebook | Fonte | Particularidade |
|---|---|---|
| `bcb/ingestion_avenue_bcb_api_olinda.py` | BCB Olinda | O mais simples: config, datas, roda |
| `bigquery/ingestion_avenue_bigquery_table.py` | GA4 via BigQuery | Laço de reprocessamento por intervalo de datas |
| `galileo/ingestion_avenue_galileo_files.py` | Arquivos Galileo | Data fixa comentada para reprocessar um dia |
| `mongodb/ingestion_evp_myprofit_prd_mongo_myprofit.py` | MongoDB MyProfit | Idêntico ao template |
| `sharepoint/ingestion_avenue_sharepoint_teamdata_itau.py` | SharePoint Itaú | Guarda de existência, falha explícita, encadeia o layer2 |

A árvore `ingestion/<service>/` é uma **reconstrução**: os `%run ../../` dos notebooks originais indicam dois níveis abaixo da raiz, mas os nomes reais das pastas intermediárias não estavam visíveis nas capturas.

## O template

Todo notebook segue a mesma sequência:

```python
%run ../../imports                                  # 1. dependências
%run ../../classes/classes                          # 2. framework
%run ../../ingestion-conf/conf_ingestion_<...>      # 3. config (define `config`)

bucket_config_prod = {                              # 4. buckets de destino
    'datalake_bucket'   : 'avenue-datalake',
    'staging_bucket'    : 'avenue-datalake-staging',
    'raw_bucket'        : 'avenue-datalake-raw',
    'databricks_bucket' : 'avenue-databricks',
}

export_date    = pydt.datetime.today()              # 5. datas
reference_date = export_date - pydt.timedelta(days=1)

pipeline = IngestionPipeline(config, bucket_config_prod)   # 6. executa
pipeline.run(export_date, reference_date)
```

`bucket_config_dev` (com os buckets prefixados por `dev-`) fica junto, comentado ou apenas não usado. A troca de ambiente é manual, editando qual dicionário é passado ao construtor.

## Padrões de reprocessamento

**Um dia específico** — comentar o `today()` e fixar a data (visível nos notebooks de Galileo e MongoDB):

```python
export_date = pydt.datetime.today()
# export_date = pydt.datetime(2025, 9, 4)
```

**Um intervalo** — gerar a lista de datas e executar em laço, **em ordem crescente** (notebook do BigQuery):

```python
days_range = ["9"]
enddate = pydt.datetime.today()
startdate = enddate - pydt.timedelta(days=int(days_range[0]))

dates_export = []
while startdate <= enddate:
    dates_export.append(startdate.strftime("%Y-%m-%d"))
    startdate += pydt.timedelta(days=1)

for date in dates_export:
    export_date = pydt.datetime.strptime(date, '%Y-%m-%d')
    reference_date = export_date - pydt.timedelta(days=1)
    pipeline = IngestionPipeline(config, bucket_config)
    pipeline.run(export_date, reference_date)
```

A ordem crescente é obrigatória: no modo `update`, cada dia lê o snapshot de staging do dia anterior. Nesse notebook, a linha que leria `config['modification_date']` está comentada e o intervalo foi fixado em 9 dias na mão.

**Uma tabela específica** — via `run_tables`:

```python
pipeline.run(export_date, reference_date, run_tables = ['minha_tabela'], except_tables = [])
```

## O notebook de SharePoint

É o mais completo e vale como referência de como um pipeline deve ser escrito. Ele faz três coisas que os outros não fazem:

1. **Verifica se o arquivo existe antes de rodar.** Tenta baixar o objeto do SharePoint e só instancia o pipeline se conseguir. A fonte é um arquivo depositado por um parceiro, sem garantia de horário — sem essa guarda, o job falharia todo dia em que o arquivo atrasasse. O custo é que o arquivo acaba baixado duas vezes: uma na verificação, outra dentro do `extract`.

2. **Falha de verdade.** `IngestionPipeline.run()` nunca levanta exceção — ele imprime o erro e retorna. Um job que só chama `run()` termina como sucesso mesmo com todas as tabelas quebradas. Este notebook checa o resultado e levanta:

   ```python
   if pipeline.run_results[item][0]['result'] != 'success':
       raise Exception("Table {} ingestion failed".format(table_name))
   ```

   O índice `[0]` só é correto porque o notebook roda uma tabela por vez.

3. **Encadeia o layer2.** Roda `IngestionLatestSnapshotPipeline` logo em seguida, na mesma execução e para a mesma tabela.

## Inspeção dos resultados

Ao final, o padrão é exibir o resultado da execução:

```python
for item in pipeline.run_results:
    display(pipeline.run_results[item])
    break
```

`run_results` é um dicionário com uma única chave (o UUID da execução); o `break` existe para pegar essa única entrada. Os mesmos registros são persistidos automaticamente em `gs://<databricks_bucket>/pipeline_result/`.
