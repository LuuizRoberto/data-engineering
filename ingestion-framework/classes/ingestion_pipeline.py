class IngestionPipeline(Pipeline):

    def __init__(self, _ingestion_config, _bucket_config, _dependencies = []):
        super().__init__(_dependencies)
        self.ingestion_config = _ingestion_config
        self.bucket_config = _bucket_config
        self.sm_client = secretmanager.SecretManagerServiceClient()
        self.data_origin = self.get_parquet_path(
                    self.ingestion_config['service'],
                    self.ingestion_config['project_id'],
                    self.ingestion_config['instance'],
                    self.ingestion_config['database'],
                    'removetable'
                ).replace('/removetable','').replace('/','_')
        self.data_extractor = DataExtractorFactory.get_extractor(self.ingestion_config)
        self.run_results = {}

        if ('mult_files' in self.ingestion_config and self.ingestion_config['mult_files'] == True):
            self.mult_files = True
        else:
            self.mult_files = False

    def run(self, export_date, reference_date, run_tables = [], except_tables = [], from_raw_file = False, from_staging = False):
        print('INFO - {} - Ingestion Pipeline Run Started'.format(str(pydt.datetime.now())))
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
                future = executor.submit(self.timed_ingest_table(self.ingest_table),
                                           table,
                                           export_date,
                                           reference_date,
                                           from_raw_file,
                                           from_staging)
                futures.append(future)
                futures_mapping[future] = table

            for future in concurrent.futures.as_completed(futures):
                ftable = futures_mapping[future]
                try:
                    start_time, end_time, result, message, df_staging, df_lake = future.result()
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
                    print('ERROR - {} - Table {} ingestion failed.'.format(str(pydt.datetime.now()), ftable))
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
        message = 'Pipeline run successful' if error_count == 0 else 'Some pipelines have failed'

        print("{} - {} - Ingestion Finished with status {} and message {}".format(result.upper(), str(pydt.datetime.now()), result, message))


    def timed_ingest_table(self, func):
        def _w(*args):
            start_time = pydt.datetime.now()
            res = func(*args)
            end_time = pydt.datetime.now()
            return start_time, end_time, *res
        return _w


    def ingest_table(self, table, export_date, reference_date, from_raw_file, from_staging):
        table_config = self.ingestion_config['tables'][table]

        print("INFO - {} - Starting table {} ingestion".format(str(pydt.datetime.now()), table))

        if (from_raw_file == True) & (from_staging == False):
            raw_path =  'gs://{}/{}/dl_export_date={}'.format(
                    self.bucket_config['raw_bucket'],
                    self.get_parquet_path(
                        self.ingestion_config['service'],
                        self.ingestion_config['project_id'],
                        self.ingestion_config['instance'],
                        self.ingestion_config['database'],
                        table
                    ),
                    export_date.strftime("%Y-%m-%d"), #today (theoretically)
                )

            df = self.data_extractor.extract_from_raw(raw_path, table_config)

            self.extracted_df = df
        elif (from_raw_file == False) & (from_staging == True):
            staging_path =  'gs://{}/{}/dl_export_date={}'.format(
                    self.bucket_config['staging_bucket'],
                    self.get_parquet_path(
                        self.ingestion_config['service'],
                        self.ingestion_config['project_id'],
                        self.ingestion_config['instance'],
                        self.ingestion_config['database'],
                        table
                    ),
                    export_date.strftime("%Y-%m-%d"), #today (theoretically)
                )

            df = self.data_extractor.extract_from_staging(staging_path)
        else:
            if self.mult_files == True:
                df, temp_data_file_name, original_source_name = self.data_extractor.extract(
                    export_date,
                    reference_date,
                    table,
                    table_config,
                    self.bucket_config['raw_bucket'])

            else:
                df, temp_data_file_name, original_source_name = self.data_extractor.extract(
                    export_date,
                    reference_date,
                    table,
                    table_config)

            self.extracted_df = df

            if (temp_data_file_name is not None) & (original_source_name is not None):
                raw_path =  '{}/dl_export_date={}/{}'.format(
                    self.get_parquet_path(
                        self.ingestion_config['service'],
                        self.ingestion_config['project_id'],
                        self.ingestion_config['instance'],
                        self.ingestion_config['database'],
                        table
                    ),
                    export_date.strftime("%Y-%m-%d"), #today (theoretically)
                    original_source_name.replace('/','_')
                )

                IngestionPipeline.upload_blob(self.storage_client, self.bucket_config['raw_bucket'], '/dbfs' + temp_data_file_name, raw_path)

        for col in df.columns:
            new_col_name = re.sub(r'[\s]+', '_', self.strip_accents(col))
            new_col_name = re.sub(r'[^a-zA-Z0-9_]+', '', new_col_name)
            new_col_name = re.sub(r'^([0-9])', r'_\1', new_col_name)
            df = df.withColumnRenamed(col, new_col_name)

        result, message, df_staging, df_lake = self.save_dataframe_to_storage(
                                                   df,
                                                   table,
                                                   table_config['extract_mode'],
                                                   export_date,
                                                   reference_date,
                                                   from_staging
                                                )

        return result, message, df_staging, df_lake

    def save_dataframe_to_storage(self, df, table, extract_mode, export_date, reference_date, from_staging):

        service = self.ingestion_config['service']
        project_id = self.ingestion_config['project_id']
        instance = self.ingestion_config['instance']
        database = self.ingestion_config['database']

        export_date_str = export_date.strftime("%Y-%m-%d")
        reference_date_str = reference_date.strftime("%Y-%m-%d")

        try:
            staging_parquet_path = self.get_parquet_path(service, project_id, instance, database, table)

            staging_parquet_table = 'gs://{}/{}'.format(
                self.bucket_config['staging_bucket']
                ,staging_parquet_path
            )

            datalake_parquet_path = self.get_parquet_path(service, project_id, instance, database, table, prefix = 'layer1/')

            datalake_parquet_path_uc = self.get_parquet_path(service, project_id, instance, database, '<table>', prefix = 'layer1/')

            datalake_parquet_table = 'gs://{}/{}'.format(
                self.bucket_config['datalake_bucket'],
                datalake_parquet_path
            )

            if from_staging == False:
                df = df.withColumn("dl_hash_key",
                    sqlfunc.sha2(
                        sqlfunc.concat_ws(
                            *([""]+ [sqlfunc.col(col).cast("string") for col in df.columns])
                        )
                        , 256
                    )
                )

                df = df.withColumn('dl_export_datetime',sqlfunc.lit(pydt.datetime.now()))

                print("INFO - {} - Saving table {}.{} in Staging: {}".format(str(pydt.datetime.now()), database, table, staging_parquet_table+'/dl_export_date='+export_date_str))
                df.write.mode('overwrite').parquet(staging_parquet_table+'/dl_export_date='+export_date_str)
                df = spark.read.parquet(staging_parquet_table+'/dl_export_date='+export_date_str)

            try:
                self.create_big_query_external_table(self.bucket_config['staging_bucket'],
                                                     staging_parquet_path,
                                                     self.bucket_config['staging_bucket'].replace('-','_'),
                                                     df,
                                                     table,
                                                     'CUSTOM',
                                                     '/{dl_export_date:DATE}')
            except Exception as e:
                print("ERROR - {} - Error creating BigQuery table {}.{}, error: {}".format(
                    str(pydt.datetime.now()),
                    self.bucket_config['staging_bucket'].replace('-','_'), staging_parquet_path,
                    str(e)
                ))

            if extract_mode == 'update':
                staging_parquet_table_last_ingestion = 'gs://{}/{}/dl_export_date={}'.format(
                    self.bucket_config['staging_bucket'],
                    self.get_parquet_path(service, project_id, instance, database, table),
                    reference_date_str
                )

                df_last_ingestion = spark.read.parquet(staging_parquet_table_last_ingestion)

                df_lake = df.alias('a').join(df_last_ingestion.alias('b'),sqlfunc.col('a.dl_hash_key') == sqlfunc.col('b.dl_hash_key'),how='left')\
                    .select([sqlfunc.col('a.'+xx).alias(xx) for xx in df.columns] + [sqlfunc.col('b.dl_hash_key').alias('b_dl_hash_key')])
                df_lake = df_lake.filter(sqlfunc.col('b_dl_hash_key').isNull())
                df_lake = df_lake.drop('b_dl_hash_key')

            if extract_mode == 'append':
                df_lake = df

            print("INFO - {} - Saving table {}.{} in Datalake in {} mode: {}".format(str(pydt.datetime.now()), database, table, extract_mode, datalake_parquet_table+'/dl_export_date='+export_date_str))
            df_lake.write.mode('overwrite').parquet(datalake_parquet_table+'/dl_export_date='+export_date_str)
            df_lake = spark.read.parquet(datalake_parquet_table+'/dl_export_date='+export_date_str)

            try:
                self.create_big_query_external_table(self.bucket_config['datalake_bucket'],
                                                     datalake_parquet_path,
                                                     self.bucket_config['datalake_bucket'].replace('-','_'),
                                                     df_lake,
                                                     table,
                                                     'CUSTOM',
                                                     '/{dl_export_date:DATE}')

            except Exception as e:
                print("ERROR - {} - Error creating BigQuery table {}.{}, error: {}".format(
                    str(pydt.datetime.now()),
                    self.bucket_config['datalake_bucket'].replace('-','_'), datalake_parquet_path,
                    str(e)
                ))

            # uc_dataset = 'av_datalake_l1'
            # if self.bucket_config['datalake_bucket'] == 'dev-avenue-datalake':
            #     uc_dataset = 'av_datalake_l1_dev'

            # try:
            #     self.create_unity_catalog_table(self.bucket_config['datalake_bucket'],
            #                                     datalake_parquet_path_uc,
            #                                     uc_dataset,
            #                                     df_lake,
            #                                     table)

            # except Exception as e:
            #     print("ERROR - {} - Error creating Unity Catalog table {}.{}, error: {}".format(
            #         str(pydt.datetime.now()),
            #         uc_dataset, datalake_parquet_path_uc.replace('-','_').replace('/','_').replace('_<table>','.'+table),
            #         str(e)
            #     ))

            return 'success', 'Pipeline execution succesfull', df, df_lake

        except Exception as e:
            print("ERROR - {} - Persisting table {}.{} to Storage in {} mode: {}".format(str(pydt.datetime.now()), database, table, extract_mode, str(e)))
#             logger.error('{};save_dataframe_to_storage;finish;error;{};{};save_dataframe_to_storage finished with error: {}'.format(step_sequence, service, database, str(e)))
            return 'failed', str(e), None, None


    def get_parquet_path(self, service, project_id, instance, database, table, prefix =  '', suffix = ''):
        return '{}{}/{}/{}/{}/{}{}'.format(
                prefix,
                service.replace('-','_').replace('.','_').replace('/','_'),
                project_id.replace('-','_').replace('.','_').replace('/','_'),
                instance.replace('-','_').replace('.','_').replace('/','_'),
                database.replace('-','_').replace('.','_').replace('/','_'),
                table.replace('-','_').replace('.','_').replace('/','_'),
                suffix)


    def create_big_query_external_table(self, bucket, parquet_path, bq_dataset, df, table, partition_mode = 'AUTO', partition_string = ''):

        decimal_target_type = None
        if table != None and 'decimal_target_type' in self.ingestion_config['tables'][table] and self.ingestion_config['tables'][table]['decimal_target_type'] != None:
            decimal_target_type = self.ingestion_config['tables'][table]['decimal_target_type']

        bq_fields = []
        for f in df.schema.fields:
            bq_type = 'STRING'
            if str(f.dataType) == 'StringType':
                    bq_type = 'STRING'
            elif str(f.dataType) == 'ByteType':
                bq_type = 'INT64'
            elif str(f.dataType) == 'ShortType':
                bq_type = 'INT64'
            elif str(f.dataType) == 'IntegerType':
                bq_type = 'INT64'
            elif str(f.dataType) == 'LongType':
                bq_type = 'INT64'
            elif str(f.dataType) == 'DoubleType':
                bq_type = 'NUMERIC'
            elif str(f.dataType) == 'FloatType':
                bq_type = 'FLOAT64'
            elif str(f.dataType).find('DecimalType') != -1:
                bq_type = 'NUMERIC'
            elif str(f.dataType) == 'TimestampType':
                bq_type = 'TIMESTAMP'
            elif str(f.dataType) == 'DateType':
                bq_type = 'DATE'
            elif str(f.dataType) == 'BooleanType':
                bq_type = 'BOOLEAN'
            elif str(f.dataType) == 'BinaryType':
                bq_type = 'BYTES'
            else:
                bq_type = 'STRING'

            bq_fields.append(bigquery.SchemaField(f.name,bq_type,'NULLABLE'))

        bq_client = bigquery.Client(project='data-prod-295413')

        dataset_ref = bq_client.dataset(bq_dataset)
        if(bq_dataset == self.bucket_config['datalake_bucket'].replace('-','_')):
            bq_table_name = parquet_path.replace('layer1/','').replace('-','_').replace('/','_')
        else:
            bq_table_name = parquet_path.replace('-','_').replace('/','_')

        table_ref = bigquery.TableReference(dataset_ref, bq_table_name)

        table_id = bigquery.Table(table_ref)

        # try:
        #     bq_client.delete_table(table_id, not_found_ok=True)  # Make an API request.
        # except Exception as e:
        #     print(e)

        try:
            ttable = bq_client.get_table(table_id)
            print("INFO - {} - Table {} already exists.".format(str(pydt.datetime.now()),table_id))
            original_schema = ttable.schema
            new_fields = []
            for field in bq_fields:
                if field.name.lower() not in [sfield.name.lower() for sfield in original_schema]:
                    original_schema.append(field)
                    new_fields.append(field.name)

            if len(new_fields) > 0:
                print("INFO - {} - Patching Table {} with new fields: {}".format(str(pydt.datetime.now()),table_id,', '.join(new_fields)))
                ttable.schema = original_schema
                bq_client.update_table(ttable ,['schema'])
            else:
                print("INFO - {} - Table {} has no new fields".format(str(pydt.datetime.now()), table_id))

        except NotFound:
            print("INFO - {} - Table {} is not found.".format(str(pydt.datetime.now()), table_id))
            external_config = bigquery.ExternalConfig('PARQUET')

            if decimal_target_type != None:
                external_config.decimal_target_types = [decimal_target_type]

            external_config.autodetect = True

            hivepartoptions = bigquery.external_config.HivePartitioningOptions()
            hivepartoptions.mode = partition_mode
            hivepartoptions.source_uri_prefix = 'gs://{}/{}'.format(bucket, parquet_path) + partition_string
            external_config.hive_partitioning = hivepartoptions

            external_config.source_uris = ['gs://{}/{}'.format(bucket, parquet_path)+ '/*']
            table_id.external_data_configuration = external_config
            # False to ignore already exists
            ttable = bq_client.create_table(table_id, False)
            print("INFO - {} - Table {} successfully created.".format(str(pydt.datetime.now()), table_id))


    def create_unity_catalog_table(self, bucket, parquet_path, uc_catalog, df, table):

        uc_schema_name = parquet_path.replace('layer1/','')\
                                      .replace('-','_')\
                                      .replace('/','_')\
                                      .replace('_<table>','').lower()

        uc_parquet_path = parquet_path.replace('<table>',table)

        uc_table_name = table.replace('-','_').replace('/','_').lower()

        def get_uc_field_type(dataType):
            uc_type = 'integer'
            if str(dataType) == 'StringType':
                uc_type = 'string'
            elif str(dataType) == 'ByteType':
                uc_type = 'integer'
            elif str(dataType) == 'ShortType':
                uc_type = 'short'
            elif str(dataType) == 'IntegerType':
                uc_type = 'integer'
            elif str(dataType) == 'LongType':
                uc_type = 'long'
            elif str(dataType) == 'DoubleType':
                uc_type = 'double'
            elif str(dataType) == 'FloatType':
                uc_type = 'float'
            elif str(dataType).find('DecimalType') != -1:
                uc_type = 'double'
            elif str(dataType) == 'TimestampType':
                uc_type = 'timestamp'
            elif str(dataType) == 'DateType':
                uc_type = 'date'
            elif str(dataType) == 'BooleanType':
                uc_type = 'boolean'
            elif str(dataType) == 'BinaryType':
                uc_type = 'binary'
            else:
                uc_type = 'string'

            return uc_type

        try:
            existing_table = spark.sql(f"DESCRIBE TABLE {uc_catalog}.{uc_schema_name}.{uc_table_name}")
            existing_columns = [row['col_name'] for row in existing_table.collect()]

            new_columns = []
            for field in df.schema.fields:
                if field.name not in [col for col in existing_columns]:
                    new_columns.append(field.name + ' ' + get_uc_field_type(field.dataType))

            if new_columns:
                for col in new_columns:
                    spark.sql(f"ALTER TABLE {uc_catalog}.{uc_schema_name}.{uc_table_name} ADD COLUMNS ({col})")
                print(f"INFO - {str(pydt.datetime.now())} - Table {uc_catalog}.{uc_schema_name}.{uc_table_name} patched with new fields: {', '.join(new_columns)}")
            else:
                print(f"INFO - {str(pydt.datetime.now())} - Table {uc_catalog}.{uc_schema_name}.{uc_table_name} has no new fields")

        except Exception as e:
            print(f"INFO - {str(pydt.datetime.now())} - Table {uc_catalog}.{uc_schema_name}.{uc_table_name} is not found or error occurred")

            spark.sql(f'CREATE SCHEMA IF NOT EXISTS {uc_catalog}.{uc_schema_name}')

            new_columns = []
            for field in df.schema.fields:
                new_columns.append(field.name + ' ' + get_uc_field_type(field.dataType))

            create_query = f'''CREATE TABLE IF NOT EXISTS {uc_catalog}.{uc_schema_name}.{uc_table_name}
            ({','.join(new_columns)})
            USING PARQUET
            LOCATION "gs://{bucket}/{uc_parquet_path}"'''
            print(create_query)
            spark.sql(create_query)

            print(f"INFO - {str(pydt.datetime.now())} - Table {uc_catalog}.{uc_schema_name}.{uc_table_name} successfully created.")


    def persist_pipeline_run_result(self):
        total_results = []
        for uuid in self.run_results:
            total_results.extend(self.run_results[uuid])

        df = spark.createDataFrame(total_results)
        df.write.partitionBy('export_date').mode('append').parquet('gs://{}/pipeline_result/layer1/ingestion'.format(self.bucket_config['databricks_bucket']))

        self.create_big_query_external_table(self.bucket_config['databricks_bucket'],
                                             'pipeline_result/layer1/ingestion',
                                             self.bucket_config['databricks_bucket'].replace('-','_'),
                                             df,
                                             None,
                                             'CUSTOM',
                                             '/{export_date:DATE}')
