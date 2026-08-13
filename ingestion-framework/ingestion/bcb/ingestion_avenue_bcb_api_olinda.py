%run ../../imports

# COMMAND ----------

%run ../../classes/classes

# COMMAND ----------

%run ../../ingestion-conf/conf_ingestion_avenue_bcb_api_olinda

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

export_date = pydt.datetime.today()
reference_date = export_date - pydt.timedelta(days=1)

pipeline = IngestionPipeline(config, bucket_config_prod)
pipeline.run(export_date, reference_date)
# pipeline.run(export_date, reference_date,run_tables = ['bcb_cdi_daily'], except_tables = [])
