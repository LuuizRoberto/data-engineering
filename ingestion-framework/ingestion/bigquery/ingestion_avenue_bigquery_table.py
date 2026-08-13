%run ../../imports

# COMMAND ----------

pip install db-dtypes

# COMMAND ----------

import db_dtypes

# COMMAND ----------

%run ../../classes/classes

# COMMAND ----------

%run ../../ingestion-conf/conf_ingestion_avenue_bigquery_table

# COMMAND ----------

print(config)

# COMMAND ----------

# days_range = config['modification_date'].split(" ")
days_range = ["9"]

enddate = pydt.datetime.today()
startdate = enddate - pydt.timedelta(days=int(days_range[0]))

dates_export = []

while startdate <= enddate:
    dates_export.append(startdate.strftime("%Y-%m-%d"))
    startdate += pydt.timedelta(days=1)

print(dates_export)

# COMMAND ----------

bucket_config_prod = {
    'datalake_bucket' : 'avenue-datalake',
    'staging_bucket' : 'avenue-datalake-staging',
    'raw_bucket' : 'avenue-datalake-raw',
    'databricks_bucket' : 'avenue-databricks',
}

bucket_config = bucket_config_prod

for date in dates_export:
    export_date = pydt.datetime.strptime(date, '%Y-%m-%d')
    reference_date = export_date - pydt.timedelta(days=1)
    pipeline = IngestionPipeline(config, bucket_config)
    pipeline.run(export_date, reference_date)

# COMMAND ----------

for item in pipeline.run_results:
    display(pipeline.run_results[item])
    break
