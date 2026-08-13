# ============================================================
# Ingestão simplificada - Jira -> Spark DataFrame
# ============================================================
import base64
import time
import requests
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

scope = "avenue-finance-ingestion-jira-contas-a-pagar"

secret_token = w.secrets.get_secret(scope=scope, key="jira-token-api-key")
JIRA_TOKEN = base64.b64decode(secret_token.value).decode("utf-8")

email = w.secrets.get_secret(scope=scope, key="jira-email-user")
JIRA_EMAIL = base64.b64decode(email.value).decode("utf-8")

workspace = w.secrets.get_secret(scope=scope, key="jira-workspace-id")
ASSETS_WORKSPACE_ID = base64.b64decode(workspace.value).decode("utf-8")

JIRA_URL    = "https://avenue-securities.atlassian.net"   # ajuste se necessário
PROJECT_KEY = "FIN"

auth_b64 = base64.b64encode(f"{JIRA_EMAIL}:{JIRA_TOKEN}".encode()).decode()
HEADERS = {
    "Authorization": f"Basic {auth_b64}",
    "Accept": "application/json",
    "Content-Type": "application/json",
}

_ASSET_CACHE = {}

# ---------- Campos necessários ----------
STANDARD_FIELDS = ["summary", "status", "created", "duedate", "resolutiondate", "reporter"]

CUSTOM_FIELDS = {
    "tipo_solicitacao":    "customfield_10826",  # Issue Customer Request Type
    "valor":               "customfield_10902",  # Issue Valor
    "centro_de_custo":     "customfield_11426",  # Issue Centro de custo (objeto Assets)
    "empresa_contratante": "customfield_10760",  # Issue Contracting Company
    "fornecedor":          "customfield_10800",  # Issue Provider (Fornecedor)
    "cnpj":                "customfield_10801",  # Issue CNPJ
}

ALL_FIELDS = STANDARD_FIELDS + list(CUSTOM_FIELDS.values())


def resolve_asset_object(raw_value):
    """Resolve campo Assets/CMDB (centro_de_custo) para um dict com label +
    atributos (Código, Nome, Gestor, Responsável Financeiro, Diretoria).
    O Jira devolve só {"objectId": "...", ...} - essa função busca o objeto
    completo na API de Assets e extrai os atributos legíveis."""
    if not raw_value:
        return None
    obj = raw_value[0] if isinstance(raw_value, list) and raw_value else raw_value
    if not isinstance(obj, dict):
        return None

    object_id = obj.get("objectId")
    if not object_id:
        return None
    if object_id in _ASSET_CACHE:
        return _ASSET_CACHE[object_id]

    url = f"https://api.atlassian.com/jsm/assets/workspace/{ASSETS_WORKSPACE_ID}/v1/object/{object_id}"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        data = resp.json()
    except requests.exceptions.HTTPError:
        _ASSET_CACHE[object_id] = None
        return None

    attrs = {}
    for attr in data.get("attributes", []):
        name = attr.get("objectTypeAttribute", {}).get("name")
        values = attr.get("objectAttributeValues", [])
        if not name or not values:
            continue
        v = values[0]
        if v.get("referencedType") and v.get("referencedObject"):
            attrs[name] = v["referencedObject"].get("label")
        else:
            attrs[name] = v.get("displayValue")

    result = {
        "label":                  data.get("label"),
        "codigo":                 attrs.get("Código"),
        "nome":                   attrs.get("Nome"),
        "diretoria":              attrs.get("Diretoria"),
        "responsavel_financeiro": attrs.get("Responsável Financeiro"),
    }
    _ASSET_CACHE[object_id] = result
    return result


def jql_search(jql: str, next_page_token: str = None, max_results: int = 50) -> dict:
    url = f"{JIRA_URL}/rest/api/3/search/jql"
    body = {"jql": jql, "maxResults": max_results, "fields": ALL_FIELDS}
    if next_page_token:
        body["nextPageToken"] = next_page_token
    resp = requests.post(url, headers=HEADERS, json=body, timeout=60)
    resp.raise_for_status()
    return resp.json()


def parse_issue(issue):
    f = issue.get("fields", {})
    centro_custo = resolve_asset_object(f.get("customfield_11426")) or {}

    return {
        "issue_key":                     issue.get("key"),
        "summary":                       f.get("summary"),
        "status":                        (f.get("status") or {}).get("name"),
        "created_at":                    f.get("created"),
        "due_date":                      f.get("duedate"),
        "resolution_date":               f.get("resolutiondate"),
        "reporter":                      (f.get("reporter") or {}).get("displayName"),
        "reporter_email":                (f.get("reporter") or {}).get("emailAddress"),
        "tipo_solicitacao":              (f.get("customfield_10826") or {}).get("value"),
        "valor":                         float(f["customfield_10902"]) if f.get("customfield_10902") is not None else None,
        "centro_de_custo_id":            f.get("customfield_11426"),
        "centro_de_custo":               centro_custo.get("label"),
        "centro_de_custo_codigo":        centro_custo.get("codigo"),
        "centro_de_custo_diretoria":     centro_custo.get("diretoria"),
        "centro_de_custo_resp_financ":   centro_custo.get("responsavel_financeiro"),
        "empresa_contratante":           (f.get("customfield_10760") or {}).get("value"),
        "fornecedor":                    f.get("customfield_10800"),
        "cnpj":                          f.get("customfield_10801"),
    }


# ---------- Loop de paginação ----------
JQL = f"project = {PROJECT_KEY} ORDER BY created ASC"

all_issues = []
next_page_token = None

while True:
    data = jql_search(JQL, next_page_token=next_page_token)
    issues = data.get("issues", [])
    if not issues:
        break

    all_issues.extend(parse_issue(i) for i in issues)

    next_page_token = data.get("nextPageToken")
    if data.get("isLast", True) or not next_page_token:
        break
    time.sleep(0.2)

print(f"Total de issues extraídas: {len(all_issues)}")


# ---------- DataFrame Spark ----------
SCHEMA = StructType([
    StructField("issue_key",                   StringType(), True),
    StructField("summary",                     StringType(), True),
    StructField("status",                      StringType(), True),
    StructField("created_at",                  StringType(), True),
    StructField("due_date",                    StringType(), True),
    StructField("resolution_date",              StringType(), True),
    StructField("reporter",                     StringType(), True),
    StructField("reporter_email",               StringType(), True),
    StructField("tipo_solicitacao",             StringType(), True),
    StructField("valor",                        DoubleType(), True),
    StructField("centro_de_custo_id",           StringType(), True),
    StructField("centro_de_custo",              StringType(), True),
    StructField("centro_de_custo_codigo",       StringType(), True),
    StructField("centro_de_custo_diretoria",    StringType(), True),
    StructField("centro_de_custo_resp_financ",  StringType(), True),
    StructField("empresa_contratante",          StringType(), True),
    StructField("fornecedor",                   StringType(), True),
    StructField("cnpj",                         StringType(), True),
])

df = spark.createDataFrame(all_issues, schema=SCHEMA)
df.display()
