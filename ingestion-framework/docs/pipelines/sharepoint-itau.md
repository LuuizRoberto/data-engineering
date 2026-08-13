# Pipeline — SharePoint Itaú

Arquivos depositados pelo Itaú em uma biblioteca do SharePoint Online da Avenue.

| | |
|---|---|
| **Notebook** | [`ingestion/sharepoint/ingestion_avenue_sharepoint_teamdata_itau.py`](../../ingestion/sharepoint/ingestion_avenue_sharepoint_teamdata_itau.py) |
| **Config** | [`ingestion-conf/conf_ingestion_avenue_sharepoint_teamdata_itau.py`](../../ingestion-conf/conf_ingestion_avenue_sharepoint_teamdata_itau.py) |
| **Extractor** | `SharepointDataExtractor` |
| **Origem** | `https://avenuesec.sharepoint.com/sites/team-data/` → `Documentos Compartilhados/DataLake/Itau/` |
| **Autenticação** | Secret Manager: `clientid-sharepoint-avenue-team-data` e `secret-sharepoint-avenue-team-data` (**versions/1**), via `ClientCredential` do Office365 |
| **Caminho no lake** | `sharepoint/avenue/teamdata/itau/<tabela>` |

Este é o pipeline de referência do repositório — o mais bem tratado, e o modelo a seguir para código novo.

## Tabelas

Todas: `ds_type: csv`, `delimiter: ';'`, `has_header: True`, `extract_mode: append`, `latest_snapshot_mode: full`, `primary_key: None`.

| Tabela | Arquivo | Colunas |
|---|---|---|
| `vinculo_clientes_ion` | `VinculoClientesIon/Clientes_Avenue_{date_int}.csv` | 6 |
| `vinculo_clientes_private` | `VinculoClientesPrivate/Clientes_Private_{date_int}.csv` | 7 |
| `liberacao_acesso_extranet` | `LiberacaoAcessoExtranet/liberacao_acesso_extranet_{date_int}.csv` | 39 |

O `{date_int}` é substituído por **`reference_date`** no formato `%Y%m%d` — o arquivo buscado é sempre o do dia anterior. Todos os campos são declarados como `STRING`, inclusive as várias colunas de data.

### Sobre o `latest_snapshot_mode: full`

O modo `full` **não faz tratamento nenhum**: o layer2 recebe o layer1 inteiro, todas as partições. Combinado com `extract_mode: append` (um arquivo completo por dia), o resultado no dataset `..._latest_snapshot` é o **histórico acumulado**, não um estado atual. O nome do dataset engana — vale avisar quem consome.

É também o motivo de `primary_key: None` ser seguro aqui: com modo `update`, causaria `AttributeError`.

## Fluxo

1. **O notebook verifica se o arquivo existe** antes de instanciar o pipeline (ver abaixo).
2. O extractor baixa o arquivo do SharePoint por caminho relativo ao servidor.
3. Detecta o encoding com `chardet`; se não for `utf-8-sig`, reescreve o arquivo convertido.
4. Lê o cabeçalho com o módulo `csv` e conta as colunas.
5. **Valida a quantidade de colunas** contra o `header` do config — levanta exceção se divergir.
6. **Valida os nomes das colunas** nos dois sentidos (arquivo → config e config → arquivo) — levanta exceção se divergir.
7. Substitui a primeira linha pelos nomes do config (normaliza o cabeçalho para a leitura).
8. Lê o CSV com schema explícito montado a partir de `header`.
9. Retorna `(df, dbfs_file_path, source_blob_name)` — há camada raw.
10. Layer1: `append`. Layer2: `full`.

## O que este pipeline faz de diferente

### Guarda de existência

A fonte é um arquivo depositado por um parceiro externo, sem garantia de horário. O notebook tenta baixar o objeto antes de rodar e só segue se conseguir:

```python
try:
    with open(full_file_path, "wb") as local_file:
        file = ctx.web.get_file_by_server_relative_url(source_blob_name).download(local_file).execute_query()
        file_exists = True
        ...
except:
    file_exists = False
    print('INFO - {} - Sharepoint object not found'.format(str(pydt.datetime.now())))
```

Sem isso, o job falharia todo dia em que o Itaú atrasasse a entrega. O custo é que o arquivo acaba baixado duas vezes: uma na verificação, outra dentro do `extract`.

### Falha explícita

É o **único** notebook que propaga erro para o job:

```python
if pipeline.run_results[item][0]['result'] != 'success':
    raise Exception("Table {} ingestion failed".format(table_name))
```

Sem esse `raise`, o job terminaria verde mesmo com a ingestão quebrada, porque `IngestionPipeline.run()` não levanta exceção. O índice `[0]` só é correto porque este notebook roda **uma tabela por vez** (`run_tables=[table_name]`).

### Encadeia o layer2

Roda `IngestionLatestSnapshotPipeline` logo após o layer1, na mesma execução e para a mesma tabela.

### Validação de contrato

É o único extractor que verifica se o arquivo recebido corresponde ao que o config declara, e o único com `extract_from_raw`, que permite reprocessar a partir da camada raw sem baixar de novo do SharePoint.

## Pontos de atenção

- **A validação de nomes compara conjuntos, não ordem.** Um arquivo com as mesmas colunas em ordem trocada passa na validação e depois é lido com schema posicional — dado certo na coluna errada. É a única brecha de uma validação que, no resto, é a melhor do framework.
- **`E-MAIL` vira `EMAIL`** depois da normalização (o hífen é removido, não vira underscore). Quem consultar a tabela não encontra `E-MAIL` nem `E_MAIL`.
- **Dado pessoal sem mascaramento**: `CLIENTE_CPF_C_DAC` (CPF), e-mails, `TELEFONE`, `CELULAR` e nomes de colaboradores. `exclude_columns` vazio.
- `chardet.detect` pode devolver `None` em arquivo vazio, o que faria `orig_encoding.lower()` estourar.
- O `type: 'csv'` da raiz do config é ignorado por este extractor, que lê o `ds_type` de cada tabela.
- Existe um segundo config de SharePoint neste framework, com a tabela `search_comments` — tratada por `if table == "search_comments"` no extractor, e o único caso que monta o nome do arquivo a partir de `export_date` (ano e mês) em vez de `reference_date`. Esse config não está incluído neste arquivo.
