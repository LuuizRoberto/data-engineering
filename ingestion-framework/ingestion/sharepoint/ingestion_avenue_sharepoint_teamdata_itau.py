%run ../../imports

# COMMAND ----------

%run ../../classes/classes

# COMMAND ----------

%run ../../ingestion-conf/conf_ingestion_avenue_sharepoint_teamdata_itau

# COMMAND ----------

def generate_temp_file_name():
     # create local temp file to store downloaded data
    temp_file = tempfile.NamedTemporaryFile(mode='w+', encoding='utf8', newline='\n', dir='/')
    temp_file_name = temp_file.name
    temp_file.close()

    return temp_file_name

def detect_file_encoding(filepath):
    """Detects the encoding of a given file."""
    with open(filepath, 'rb') as f:
        # Read a sufficient amount of data to allow chardet to analyze
        raw_data = f.read(100000)  # Read up to 100KB for better accuracy
        result = chardet.detect(raw_data)
        return result['encoding']

# COMMAND ----------

bucket_config_prod = {
    'datalake_bucket' : 'avenue-datalake',
    'staging_bucket' : 'avenue-datalake-staging',
    'raw_bucket' : 'avenue-datalake-raw',
    'databricks_bucket' : 'avenue-databricks',
}

bucket_config_dev = {
    'datalake_bucket' : 'dev-avenue-datalake',
    'staging_bucket' : 'dev-avenue-datalake-staging',
    'raw_bucket' : 'dev-avenue-datalake-raw',
    'databricks_bucket' : 'dev-avenue-databricks',
}

table_name = 'liberacao_acesso_extranet'

# [restante da célula 5 não visível no screenshot de origem — inclui a definição de export_date e reference_date]

# COMMAND ----------

file_exists = False

sm_user_secret_id = config['sec']['sm_user_secret_id']
sm_pwd_secret_id = config['sec']['sm_pwd_secret_id']
site_url = config['sec']['site_url']

endpoint = config['tables'][table_name]['endpoint']

source_blob_name = endpoint.replace('{'+config['tables'][table_name]['filename_date_field']+'}'
                                   ,reference_date.strftime(config['tables'][table_name]['filename_date_format']))

sm_client = secretmanager.SecretManagerServiceClient()

sm_user  = sm_client.access_secret_version(request={"name": "projects/data-prod-295413/secrets/{}/versions/1".format(sm_user_secret_id)}).payload.data.decode("UTF-8")

sm_pwd   = sm_client.access_secret_version(request={"name": "projects/data-prod-295413/secrets/{}/versions/1".format(sm_pwd_secret_id)}).payload.data.decode("UTF-8")

client_credentials = ClientCredential('{client_id}'.format(client_id = sm_user)
                                     ,'{client_secret}'.format(client_secret = sm_pwd))

ctx = ClientContext(site_url).with_credentials(client_credentials)

temp_data_file_name = generate_temp_file_name() + '.{}'.format(config['tables'][table_name]['ds_type'])
dbfs_folder = '/tmp'
dbfs_file_path = dbfs_folder + temp_data_file_name
full_file_path = '/dbfs' + dbfs_file_path

print('INFO - Sharepoint object {}'.format(source_blob_name))
print('INFO - {} - Checking if sharepoint object exists'.format(str(pydt.datetime.now())))

try:
    with open(full_file_path, "wb") as local_file:
        file = ctx.web.get_file_by_server_relative_url(source_blob_name).download(local_file).execute_query()

        print('INFO - {} - Sharepoint object exists'.format(str(pydt.datetime.now())))
        file_exists = True

        orig_encoding = detect_file_encoding(full_file_path)
        print('INFO - {} - The detected encoding for sharepoint object is: {}'.format(str(pydt.datetime.now())
                                                                                    ,orig_encoding.lower()))
except:
    file_exists = False
    print('INFO - {} - Sharepoint object not found'.format(str(pydt.datetime.now())))

ctx = None

# COMMAND ----------

if file_exists is True:
    pipeline = IngestionPipeline(config, bucket_config_prod)
    pipeline.run(export_date, reference_date, run_tables = [table_name], except_tables = [])

    for item in pipeline.run_results:
        display(pipeline.run_results[item])
        break

    if pipeline.run_results[item][0]['result'] != 'success':
        raise Exception("Table {} ingestion failed".format(table_name))

# COMMAND ----------

if file_exists is True:
    pipelinels = IngestionLatestSnapshotPipeline(config, bucket_config_prod)
    pipelinels.run(export_date, reference_date, run_tables = [table_name], except_tables = [])

    for item in pipelinels.run_results:
        display(pipelinels.run_results[item])
        break
