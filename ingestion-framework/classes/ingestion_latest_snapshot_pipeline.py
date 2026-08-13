class IngestionLatestSnapshotPipeline(Pipeline):

    def __init__(self, _ingestion_config, _bucket_config, _dependencies = []):
        super().__init__(_dependencies)
        self.ingestion_config = _ingestion_config
        self.bucket_config = _bucket_config
        self.data_origin = self.get_parquet_path(
                    self.ingestion_config['service'],
                    self.ingestion_config['project_id'],
                    self.ingestion_config['instance'],
                    self.ingestion_config['database'],
                    'removetable'
                ).replace('/removetable','').replace('/','_')
        self.run_results = {}
        self.layer2_prefix = 'layer2/ingestion_latest_snapshot/'


    def get_parquet_path(self, service, project_id, instance, database, table, prefix =  '', suffix = ''):
        return '{}{}/{}/{}/{}/{}{}'.format(
                prefix,
                service.replace('-','_').replace('.','_').replace('/','_'),
                project_id.replace('-','_').replace('.','_').replace('/','_'),
                instance.replace('-','_').replace('.','_').replace('/','_'),
                database.replace('-','_').replace('.','_').replace('/','_'),
                table.replace('-','_').replace('.','_').replace('/','_'),
                suffix)


    def run(self, export_date, reference_date, run_tables = [], except_tables = []):
        print('INFO - {} - Ingestion Latest Snapshot Pipeline Run Started'.format(str(pydt.datetime.now())))
#         print('INFO - {} - Export Date is: {}'.format(str(pydt.datetime.now(), export_date.strftime('%Y-%m-%d'))))
#         print('INFO - {} - Reference Date is: {}'.format(str(pydt.datetime.now(), reference_date.strftime('%Y-%m-%d'))))
        error_count = 0
        run_uuid = str(uuid.uuid1())
        self.run_results = {
            run_uuid : []
        }

        if len(run_tables) > 0:
            tables = [t for t in self.ingestion_config['tables'] if (t in run_tables) & (t not in except_tables)]
        else:
            tables = [t for t in self.ingestion_config['tables'] if t not in except_tables]



        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = []
            futures_mapping = {}
            for table in tables:
                future = executor.submit(self.timed_snapshot_table(self.snapshot_table),
                                           table,
                                           export_date,
                                           reference_date)
                futures.append(future)
                futures_mapping[future] = table

            for future in concurrent.futures.as_completed(futures):
                ftable = futures_mapping[future]
                try:
                    start_time, end_time, result, message, df_snapshot = future.result()
                    print('INFO - {} - Table  ingestion thread result: {}'.format(str(pydt.datetime.now()), result))

                    self.run_results[run_uuid].append(
                        {
                            "run_id" : run_uuid,
                            "table" : ftable,
                            "result" : result,
                            "message": message,
                            "start_time" : start_time,
                            "end_time" : end_time,
                            "data_origin" : self.data_origin,
                            "reference_date" : reference_date.date(),
                            "export_date" : export_date.date(),
                            "table_config" : json.dumps(self.ingestion_config['tables'][ftable])
                        }
                    )

                except Exception as e:
                    print('ERROR - {} - Table {} ingestion failed.'.format(str(pydt.datetime.now()), table))
                    # or
                    #print(sys.exc_info()[2])
                    self.run_results[run_uuid].append(
                        {
                            "run_id" : run_uuid,
                            "table" : ftable,
                            "result" : 'failed',
                            "message": str(traceback.format_exc()),
                            "start_time" : pydt.datetime.now(),
                            "end_time" : pydt.datetime.now(),
                            "data_origin" : self.data_origin,
                            "reference_date" : reference_date.date(),
                            "export_date" : export_date.date(),
                            "table_config" : json.dumps(self.ingestion_config['tables'][table])
                        }
                    )

                    error_count += 1

        self.persist_pipeline_run_result()

        result = 'success' if error_count == 0 else 'error'
        message = 'Latest Snapshot Pipeline run successful' if error_count == 0 else 'Some latest snapshot pipelines have failed'

        print("{} - {} - Latest Snapshot Finished with status {} and message {}".format(result.upper(), str(pydt.datetime.now()), result, message))


    def timed_snapshot_table(self, func):
        def _w(*args):
            start_time = pydt.datetime.now()
            res = func(*args)
            end_time = pydt.datetime.now()
            return start_time, end_time, *res
        return _w

    def snapshot_table(self, table, export_date, reference_date):

        service = self.ingestion_config['service']
        project_id = self.ingestion_config['project_id']
        instance = self.ingestion_config['instance']
        database = self.ingestion_config['database']
        ds_type = self.ingestion_config['type']

        table_config = self.ingestion_config['tables'][table]
        extract_mode = table_config['extract_mode']

        print("INFO - {} - Starting table {} latest snapshot".format(str(pydt.datetime.now()), table))

        datalake_parquet_path = self.get_parquet_path(service, project_id, instance, database, table, prefix = 'layer1/')
        datalake_parquet_table = 'gs://{}/{}'.format(
            self.bucket_config['datalake_bucket'],
            datalake_parquet_path
        )

        datalake_layer2_parquet_path = self.get_parquet_path(service, project_id, instance, database, table, prefix = self.layer2_prefix)
        datalake_layer2_parquet_table = 'gs://{}/{}'.format(
            self.bucket_config['datalake_bucket'],
            datalake_layer2_parquet_path
        )

        bq_table = self.get_parquet_path(service, project_id, instance, database, table).replace('/','_')

        #Create latest snapshot dataframe
        most_recent_export_df = spark.read.option("mergeSchema","true").parquet(datalake_parquet_table)

        primary_key = None
        if ('primary_key' in table_config):
            if (table_config['primary_key'] != None) & (table_config['primary_key'] != ''):
                primary_key = table_config['primary_key']

        if ('latest_snapshot_mode' not in table_config):
            print('latest_snapshot_mode -> extract_mode: {}'.format(table_config['extract_mode']))
            if (extract_mode == 'append'):
                most_recent_export = most_recent_export_df.select(sqlfunc.max(sqlfunc.col('dl_export_date')).alias('max_dl_export_date')).collect()
                most_recent_export_df = most_recent_export_df.filter(sqlfunc.col('dl_export_date') == sqlfunc.lit(most_recent_export[0]['max_dl_export_date']))

            if (extract_mode == 'update'):
                if  (service == 'datastore'):
                    if(table_config['primary_key'] == None):
                        window = Window.partitionBy(sqlfunc.col('key_name'),sqlfunc.col('key_id')).orderBy(sqlfunc.col('dl_export_datetime').desc())
                    else:
                        window = Window.partitionBy(*[key.strip() for key in primary_key.split(',')]).orderBy(sqlfunc.col('dl_export_datetime').desc())
                else:
                    window = Window.partitionBy(*[key.strip() for key in primary_key.split(',')]).orderBy(sqlfunc.col('dl_export_datetime').desc())

                most_recent_export_df = most_recent_export_df.withColumn('is_last', sqlfunc.row_number().over(window))
                most_recent_export_df = most_recent_export_df.filter(sqlfunc.col('is_last') == sqlfunc.lit(1))
                most_recent_export_df = most_recent_export_df.drop('is_last')

        else:
            if (table_config['latest_snapshot_mode'] == 'append'):
                print('latest_snapshot_mode: {}'.format(table_config['latest_snapshot_mode']))
                most_recent_export = most_recent_export_df.select(sqlfunc.max(sqlfunc.col('dl_export_date')).alias('max_dl_export_date')).collect()
                most_recent_export_df = most_recent_export_df.filter(sqlfunc.col('dl_export_date') == sqlfunc.lit(most_recent_export[0]['max_dl_export_date']))

            if (table_config['latest_snapshot_mode'] == 'update'):
                print('latest_snapshot_mode: {}'.format(table_config['latest_snapshot_mode']))
                if  (service == 'datastore'):
                    window = Window.partitionBy(sqlfunc.col('key_name'),sqlfunc.col('key_id')).orderBy(sqlfunc.col('dl_export_datetime').desc())
                else:
                    window = Window.partitionBy(*[key.strip() for key in primary_key.split(',')]).orderBy(sqlfunc.col('dl_export_datetime').desc())

                most_recent_export_df = most_recent_export_df.withColumn('is_last', sqlfunc.row_number().over(window))
                most_recent_export_df = most_recent_export_df.filter(sqlfunc.col('is_last') == sqlfunc.lit(1))
                most_recent_export_df = most_recent_export_df.drop('is_last')

            if (table_config['latest_snapshot_mode'] == 'full'):
                print('latest_snapshot_mode: {}'.format(table_config['latest_snapshot_mode']))
                #não precisa fazer nenhum tratamento, basta gravar o most_recent_export_df

        print("INFO - {} - Saving table {} in Datalake - Layer 2: {}".format(str(pydt.datetime.now()), table, datalake_layer2_parquet_table))

        #Save it!
        most_recent_export_df.write.mode('overwrite').parquet(datalake_layer2_parquet_table)

        most_recent_export_df = spark.read.parquet(datalake_layer2_parquet_table)

        result = 'success'
        message =  'table {} Latest Snapshot successful'.format(table)

        #Create Big Query table
        try:
            self.create_big_query_external_table(self.bucket_config['datalake_bucket'],
                                                 datalake_layer2_parquet_path,
                                                 self.bucket_config['datalake_bucket'].replace('-','_')+'_latest_snapshot',
                                                 most_recent_export_df,
                                                 table)

        except Exception as e:
            result = 'warning'
            message = "ERROR - {} - Error creating BigQuery table {}.{}, error: {}".format(
                str(pydt.datetime.now()),
                self.bucket_config['datalake_bucket'].replace('-','_')+'_latest_snapshot',
                parquet_path.replace(self.layer2_prefix,'').replace('-','_').replace('/','_'),
                str(e)
            )
            print(message)

        print("INFO - {} - Finished table {} latest snapshot".format(str(pydt.datetime.now()), table))

        #result, message, snapshot dataframe
        return result, message, most_recent_export_df


    def create_big_query_external_table(self, bucket, parquet_path, bq_dataset, df, table):

        decimal_target_type = None
        if table != None and 'decimal_target_type' in self.ingestion_config['tables'][table] and self.ingestion_config['tables'][table]['decimal_target_type'] != None:
            decimal_target_type = self.ingestion_config['tables'][table]['decimal_target_type']

        bq_client = bigquery.Client(project='data-prod-295413')

        dataset_ref = bq_client.dataset(bq_dataset)
        if(bq_dataset == self.bucket_config['datalake_bucket'].replace('-','_')+'_latest_snapshot'):
            bq_table_name = parquet_path.replace(self.layer2_prefix,'').replace('-','_').replace('/','_')
        else:
            bq_table_name = parquet_path.replace('-','_').replace('/','_')

        table_ref = bigquery.TableReference(dataset_ref, bq_table_name)

        table_id = bigquery.Table(table_ref)

        try:
            bq_client.delete_table(table_id, not_found_ok=True)  # Make an API request.
        except Exception as e:
            print(e)

        try:
            external_config = bigquery.ExternalConfig('PARQUET')

            if decimal_target_type != None:
                external_config.decimal_target_types = [decimal_target_type]

            external_config.autodetect = True
            external_config.source_uris = ['gs://{}/{}'.format(bucket, parquet_path)+ '/*']

            if bq_dataset == 'avenue_databricks' or bq_dataset == 'dev_avenue_databricks':
                hivepartoptions = bigquery.external_config.HivePartitioningOptions()
                hivepartoptions.mode = 'CUSTOM'
                hivepartoptions.source_uri_prefix = 'gs://{}/{}'.format(bucket, parquet_path) + '/{export_date:DATE}'
                external_config.hive_partitioning = hivepartoptions

            table_id.external_data_configuration = external_config

            # False to ignore already exists
            ttable = bq_client.create_table(table_id, False)
            print("INFO - {} - Table {} successfully created.".format(str(pydt.datetime.now()), table_id))

        except Exception as e:
            print("ERROR - {} - Error creating BigQuery Table: {}.".format(str(pydt.datetime.now()), str(e)))


    def persist_pipeline_run_result(self):
        total_results = []
        for uuid in self.run_results:
            total_results.extend(self.run_results[uuid])

        df = spark.createDataFrame(total_results)
        df.write.partitionBy('export_date').mode('append').parquet('gs://{}/pipeline_result/layer2/ingestion_latest_snapshot'.format(self.bucket_config['databricks_bucket']))

        self.create_big_query_external_table(self.bucket_config['databricks_bucket'],
                                             'pipeline_result/layer2/ingestion_latest_snapshot',
                                             self.bucket_config['databricks_bucket'].replace('-','_'),
                                             df,
                                             None
                                            )
