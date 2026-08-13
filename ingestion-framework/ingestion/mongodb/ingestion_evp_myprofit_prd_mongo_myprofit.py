# [célula 1 não visível no screenshot de origem]

# COMMAND ----------

%run ../../imports

# COMMAND ----------

%run ../../classes/classes

# COMMAND ----------

%run ../../ingestion-conf/conf_ingestion_evp_myprofit_prd_mongo_myprofit

# COMMAND ----------

print(config)

# COMMAND ----------

# bucket_config_dev = {
#     'datalake_bucket' : 'dev-avenue-datalake',
#     'staging_bucket' : 'dev-avenue-datalake-staging',
#     'raw_bucket' : 'dev-avenue-datalake-raw',
#     'databricks_bucket' : 'dev-avenue-databricks',
# }

bucket_config_prod = {
    'datalake_bucket' : 'avenue-datalake',
    'staging_bucket' : 'avenue-datalake-staging',
    'raw_bucket' : 'avenue-datalake-raw',
    'databricks_bucket' : 'avenue-databricks',
}

# export_date = pydt.datetime(2023,8,9)
export_date = pydt.datetime.today()
reference_date = export_date - pydt.timedelta(days=1)

pipeline = IngestionPipeline(config, bucket_config_prod)
pipeline.run(export_date, reference_date)

# COMMAND ----------

for item in pipeline.run_results:
    display(pipeline.run_results[item])
    break
