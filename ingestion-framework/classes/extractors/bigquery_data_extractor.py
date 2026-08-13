class BigQueryDataExtractor(DataExtractor):

    def __init__(self, project_id, ds_type):

        self.project_id = project_id

        self.bq_client = bigquery.Client(project = self.project_id)

        self.ds_type = ds_type

    def extract(self, export_date, reference_date, table, table_config):

        extract_mode = table_config['extract_mode']
        endpoint = table_config['endpoint']
        dataset = table_config['dataset']
        col_escape_char = table_config['col_escape_char']
        struct_fields = table_config['struct_fields']
        integer_fields = table_config['integer_fields']
        float_fields = table_config['float_fields']
        timestamp_fields = table_config['timestamp_fields']


        if self.ds_type == 'googleanalytics':
            refdate = reference_date.strftime('%Y%m%d')
            query = ("SELECT * FROM `{project_id}.{database}.{table_name}_{refdate}`").format(project_id = self.project_id,
                                                                                                database = dataset,
                                                                                                table_name = endpoint,
                                                                                                refdate = refdate)
            print(query)

        else:
            if (table_config['changed_date'] == None) | (table_config['changed_range'] == None):
                    query = ("SELECT * FROM `{project_id}.{database}.{table_name}`").format(project_id = self.project_id,
                                                                                            database = dataset,
                                                                                            table_name = table)
                    print(query)


            elif (table_config['changed_date'] != None) & (table_config['changed_range'] != None):
                changed_range = table_config['changed_range'].split(" ")

                print("INFO - Filtering last {} {}".format(changed_range[0], changed_range[1]))

                startdate = export_date - pydt.timedelta(days = int(changed_range[0]))
                enddate = export_date

                startdate_str = startdate.strftime("%Y-%m-%d")
                enddate_str = enddate.strftime("%Y-%m-%d")

                query = "SELECT * FROM {project_id}.{database}.{table}\
                        WHERE {esc}{changed_date}{esc} >= '{start}' AND {esc}{changed_date}{esc} < '{end}'".format(
                            project_id  = self.project_id,
                            database=dataset,
                            table=endpoint,
                            changed_date=table_config['changed_date'],
                            start=startdate_str,
                            end=enddate_str,
                            esc=col_escape_char)

                print(query)

        if self.ds_type == 'googleanalytics':
            df = spark.read.format('bigquery') \
            .option('project', self.project_id) \
            .option("table", "{database}.{table_name}_{refdate}".format(database = dataset,
                                                                        table_name = endpoint,
                                                                        refdate = refdate)) \
            .option('sqlQuery', query) \
            .option('fetchSize', 1000000) \
            .load()

        else:
            df = spark.read.format('bigquery') \
            .option('project', self.project_id) \
            .option("table", "{database}.{table_name}".format(database = dataset, table_name = endpoint))\
            .option('sqlQuery', query) \
            .option('viewsEnabled', 'true') \
            .load()

        print('INFO - {} - Creating DataFrame from BigQuery Connection'.format(str(pydt.datetime.now())))

        print(df.schema.fields)

        for f in df.schema.fields:
            field = f.name
            if f.name in struct_fields:
                df = df.withColumn(field, to_json(field))
            elif f.name in integer_fields:
                df = df.withColumn(field,sqlfunc.col(field).cast('integer'))
            elif f.name in float_fields:
                df = df.withColumn(field,sqlfunc.col(field).cast('float'))
            elif f.name in timestamp_fields:
                df = df.withColumn(field,sqlfunc.col(field).cast('timestamp'))
            else:
                df = df.withColumn(field,sqlfunc.col(field).cast('string'))

        print('INFO - {} - Finished BigQuery table extraction: {} '.format(str(pydt.datetime.now()), table))

        #dataframe, dbfs_path, original_source_name
        return df, None, None
