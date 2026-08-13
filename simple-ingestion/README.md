# simple-ingestion/

Scripts de ingestão **avulsos** — um arquivo, sem classes, sem config declarativo, sem camadas.

São o contraponto do [`ingestion-framework/`](../ingestion-framework/): quando a fonte é única, o volume é pequeno e não há necessidade de reprocessamento por data nem de rastreio de execução, um script direto resolve. Nada aqui herda de `Pipeline` ou `DataExtractor`, e nada aqui usa a `DataExtractorFactory`.

Como no resto do repositório, **o código está preservado verbatim**.

## Quando usar cada abordagem

| | `simple-ingestion/` | `ingestion-framework/` |
|---|---|---|
| Estrutura | script único, procedural | classes, factory, config declarativo |
| Camadas | nenhuma — DataFrame em memória | raw → staging → layer1 → layer2 |
| Config | constantes no topo do arquivo | dicionário `config` em arquivo separado |
| Segredos | Databricks Secret Scopes (`WorkspaceClient`) | GCP Secret Manager |
| Reprocessamento | reexecutar o script | `export_date` / `reference_date`, backfill por laço |
| Registro de execução | nenhum | `pipeline_result/` + tabela no BigQuery |
| Paralelismo | nenhum | uma thread por tabela |

## Scripts

### `ingestion_jira_contas_a_pagar.py`

Extrai as issues do projeto **FIN** (contas a pagar) do Jira da Avenue e monta um DataFrame Spark.

| | |
|---|---|
| **Origem** | `https://avenue-securities.atlassian.net` — API REST v3 (`/rest/api/3/search/jql`) + API de Assets |
| **Projeto Jira** | `FIN` |
| **Autenticação** | Basic Auth (e-mail + API token), via Databricks Secret Scope `avenue-finance-ingestion-jira-contas-a-pagar` |
| **Saída** | `df.display()` — **não persiste em lugar nenhum** |

#### Campos extraídos

Padrão do Jira: `summary`, `status`, `created`, `duedate`, `resolutiondate`, `reporter` (nome e e-mail).

Campos customizados:

| Campo | ID | Conteúdo |
|---|---|---|
| `tipo_solicitacao` | `customfield_10826` | Customer Request Type |
| `valor` | `customfield_10902` | valor da solicitação (único campo numérico) |
| `centro_de_custo` | `customfield_11426` | objeto do Assets/CMDB — ver abaixo |
| `empresa_contratante` | `customfield_10760` | Contracting Company |
| `fornecedor` | `customfield_10800` | Provider |
| `cnpj` | `customfield_10801` | CNPJ do fornecedor |

#### Resolução do centro de custo (Assets/CMDB)

É a parte não trivial do script. O Jira devolve o campo `customfield_11426` apenas como uma referência (`{"objectId": "..."}`), sem os dados legíveis. `resolve_asset_object` faz uma segunda chamada — agora na API de Assets, em `api.atlassian.com/jsm/assets/workspace/{workspace_id}/v1/object/{object_id}` — e achata os atributos do objeto em colunas:

```
centro_de_custo               ← label do objeto
centro_de_custo_codigo        ← atributo "Código"
centro_de_custo_diretoria     ← atributo "Diretoria"
centro_de_custo_resp_financ   ← atributo "Responsável Financeiro"
```

Atributos que referenciam outros objetos são resolvidos pelo `label` do objeto referenciado; os demais usam o `displayValue`.

O dicionário `_ASSET_CACHE` evita repetir a chamada para o mesmo `objectId` — importante, porque a resolução acontece **por issue**, e centros de custo se repetem muito entre as issues. Objetos que falham na resolução são cacheados como `None`, então não são tentados de novo na mesma execução.

#### Paginação

Usa o endpoint novo `/rest/api/3/search/jql`, com `nextPageToken` em vez do `startAt` clássico. O laço para quando não vêm mais issues, quando `isLast` é verdadeiro ou quando não há próximo token. Pausa de 0,2 s entre páginas, com 50 issues por página.

#### Segredos

Diferente do framework, que usa o Secret Manager do GCP, aqui os segredos vêm dos **Secret Scopes do Databricks**:

```python
w = WorkspaceClient()
secret_token = w.secrets.get_secret(scope=scope, key="jira-token-api-key")
JIRA_TOKEN = base64.b64decode(secret_token.value).decode("utf-8")
```

O `base64.b64decode` é necessário porque o SDK do Databricks devolve o valor codificado em base64 — não é uma camada extra de ofuscação aplicada pelo script.

Chaves do scope `avenue-finance-ingestion-jira-contas-a-pagar`: `jira-token-api-key`, `jira-email-user`, `jira-workspace-id`.

---

## Pontos de atenção

Observações da leitura do código. **Nada foi corrigido** — o script está como estava.

- **`centro_de_custo_id` tem tipo incompatível com o schema.** O campo recebe `f.get("customfield_11426")` cru, que é a estrutura do Assets (lista de dicts), mas o `SCHEMA` declara `StringType`. Se qualquer issue tiver centro de custo preenchido, o `spark.createDataFrame` levanta `TypeError`. O caminho só passa enquanto o campo vier nulo em todas as issues.
- **Não persiste nada.** Termina em `df.display()`. É um script de exploração, não um pipeline agendado — não há escrita em tabela, arquivo ou volume.
- **Extração sempre integral.** O JQL é `project = FIN ORDER BY created ASC`, sem filtro incremental: toda execução varre o projeto inteiro.
- **`resolve_asset_object` só captura `HTTPError`.** Timeout, erro de conexão ou JSON inválido escapam e derrubam a execução inteira, sem cachear o `objectId`.
- **`float(f["customfield_10902"])` sem proteção.** Se o campo vier com conteúdo não numérico, estoura `ValueError` no meio da paginação, perdendo o que já foi coletado.
- **Tudo é acumulado em memória** (`all_issues`) antes de virar DataFrame. Funciona no volume atual do projeto FIN; não escala para projetos grandes.
- **Datas ficam como string.** `created_at`, `due_date` e `resolution_date` são declaradas `StringType`, então chegam no formato ISO do Jira, sem conversão.
- **`JIRA_URL` está hardcoded** com o comentário `# ajuste se necessário`.
- `CUSTOM_FIELDS` é usado apenas para montar `ALL_FIELDS`; o `parse_issue` referencia os IDs `customfield_*` diretamente, então renomear uma chave do dicionário não tem efeito nenhum.
- O `nome` extraído em `resolve_asset_object` é calculado e não aparece no retorno de `parse_issue` — o docstring também menciona um atributo `Gestor` que não é lido.
