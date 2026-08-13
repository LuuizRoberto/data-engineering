# classes/extractors/

Extractors concretos. Cada um herda de `DataExtractor` e implementa `extract(export_date, reference_date, table, table_config)`.

No workspace original estas classes vivem no mesmo notebook `classes/classes` (algumas já haviam sido separadas em notebooks próprios, como `SharepointDataExtractor`). O código aqui é literal, sem imports próprios — ver [../README.md](../README.md).

## Os cinco extractors deste repositório

| Arquivo | Fonte | Formato intermediário | Camada raw? | Chave de schema no config |
|---|---|---|---|---|
| `bcb_data_extractor.py` | APIs públicas do Banco Central (Olinda e SGS) | JSON Lines | sim | `schema` (**ignorado**, ver abaixo) |
| `galileo_data_extractor.py` | Arquivos PGP no GCS | CSV decriptado | sim | `schema` |
| `mongodb_data_extractor.py` | MongoDB Atlas | — (direto para DataFrame) | não | — |
| `bigquery_data_extractor.py` | BigQuery (GA4 e tabelas comuns) | — (direto para DataFrame) | não | — |
| `sharepoint_data_extractor.py` | SharePoint Online | CSV ou XLSX | sim | `header` |

O framework completo declara **23 extractors** na `DataExtractorFactory`; os outros 18 não estão neste arquivo. A Factory está incluída inteira, então o contrato de construção de todos eles está preservado.

## Mapeamento de tipos — diverge entre extractors

Cada extractor traduz a string de tipo do config para um tipo Spark com sua própria cadeia de `if/elif`, e **as cadeias não são iguais**. Tipos não reconhecidos caem no `else` e viram `StringType`, sem erro nem aviso.

| Tipo no config | BCB | Galileo | SharePoint |
|---|---|---|---|
| `STRING` | ✅ | ✅ | ✅ |
| `INT` | ✅ | ✅ | ✅ |
| `DOUBLE` | ✅ | ✅ | ✅ |
| `TIMESTAMP` | ✅ | ✅ | ✅ |
| `DATE` | ✅ | ✅ | ✅ |
| `BOOLEAN` | ⚠️ presente, mas quebrado (`BooeleanType`) | vira STRING | vira STRING |
| `FLOAT` | vira STRING | vira STRING | vira STRING |
| `NUMBER` | vira STRING | vira STRING | vira STRING |

Isso tem efeito real: os configs do Galileo declaram `FLOAT` e `NUMBER` em dezenas de campos monetários, e todos chegam ao lake como texto. Ver [../../docs/known-issues.md](../../docs/known-issues.md).

`MongoDBDataExtractor` e `BigQueryDataExtractor` não usam esse mecanismo — tipam por listas de campos (`double_fields`, `integer_fields`, …).

## Notas por extractor

### BCBDataExtractor

- Duas APIs distintas atendidas pela mesma classe, escolhidas por `if table == 'eur_ptax'`: Olinda (datas `MM-DD-YYYY`, resposta em `['value']`) e SGS (datas `DD/MM/YYYY`, resposta na raiz).
- **É o único que interpreta a unidade de `changed_range`**: `days` com `timedelta`, `Months` (com M maiúsculo) com `relativedelta`.
- O `df_schema` é construído, impresso e **nunca usado** — a leitura é `spark.read.json`, que infere o schema. A chave `schema` do config é decorativa aqui.
- O arquivo raw é gravado com extensão `.csv`, mas o conteúdo é JSON Lines.

### GalileoDataExtractor

- Baixa o `.csv.gpg` do bucket do fornecedor, escreve a chave privada em `/tmp/private_key_galileo.asc` e decripta com `gnupg`.
- Passphrase e chave privada vêm do Secret Manager em **`versions/2` e `versions/3`**, não `latest`.
- Filtra registros de trailer (`TTRAILER`) e linhas totalmente nulas.
- `replace_header` substitui a primeira linha do arquivo pelos nomes do `schema`.
- Lê CSV **com schema explícito**: a ordem das colunas do arquivo precisa bater exatamente com a ordem do `schema`.

### MongoDBDataExtractor

Apesar do nome genérico, é **específico da coleção `CustomerAccount`**: `CustomerId`, `_id` e o `posexplode` de `AccountMoviment` estão fixos no código, assim como o filtro por `PositionDate`. O grão da saída é o movimento, não a conta.

A URI de conexão é montada a partir dos campos do config:

```
{service}+srv://{user}:{pwd}@{instance}.a4flz.{type}.net/{database}
→ mongodb+srv://…@evp-myprofit-prd.a4flz.mongodb.net/MyProfitEVP
```

O identificador de cluster `a4flz` está hardcoded — a classe só serve para esse cluster.

### BigQueryDataExtractor

- Dois modos, por `ds_type`: `googleanalytics` (tabela diária sufixada por `reference_date`) e o genérico.
- **Cast padrão destrutivo:** todo campo fora de `struct_fields` / `integer_fields` / `float_fields` / `timestamp_fields` é convertido para **string**.
- `struct_fields` são serializados com `to_json`.
- É o único extractor que de fato **usa o valor** de `changed_date` — como nome da coluna do filtro incremental.

### SharepointDataExtractor

O mais defensivo do conjunto, e o modelo recomendado para código novo:

- detecta o encoding com `chardet` e converte para `utf-8-sig` quando necessário;
- valida a **quantidade** de colunas contra o config e levanta exceção se divergir;
- valida os **nomes** das colunas nos dois sentidos e levanta exceção;
- suporta CSV e XLSX (o XLSX depende da biblioteca spark-excel instalada no cluster);
- é o único com `extract_from_raw`, que permite reprocessar a partir da camada raw;
- usa a chave `header` no config, não `schema`.
