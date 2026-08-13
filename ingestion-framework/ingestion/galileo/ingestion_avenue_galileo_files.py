%run ../../imports

# COMMAND ----------

%run ../../classes/classes

# COMMAND ----------

%run ../../ingestion-conf/conf_ingestion_avenue_galileo_files

# COMMAND ----------

print(config)

# COMMAND ----------

# bucket_config_dev = {
#     'datalake_bucket' : 'dev-avenue-datalake',
#     'staging_bucket' : 'dev-avenue-datalake-staging',
#     'raw_bucket' : 'dev-avenue-datalake-raw',
#     'databricks_bucket' : 'dev-avenue-databricks',
#     'environment': 'dev'
# }

bucket_config_prod = {
    'datalake_bucket' : 'avenue-datalake',
    'staging_bucket' : 'avenue-datalake-staging',
    'raw_bucket' : 'avenue-datalake-raw',
    'databricks_bucket' : 'avenue-databricks'
}

bucket_config = bucket_config_prod

export_date = pydt.datetime.today()
# export_date = pydt.datetime(2025, 9, 4)
reference_date = export_date - pydt.timedelta(days=1)

pipeline = IngestionPipeline(config, bucket_config)
pipeline.run(export_date, reference_date)
