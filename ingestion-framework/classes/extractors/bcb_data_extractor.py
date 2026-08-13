class BCBDataExtractor(DataExtractor):

    def __init__(self, _host):

        self.bcb_host = _host

    def extract(self, export_date, reference_date, table, table_config):

        extract_mode = table_config['extract_mode']
        endpoint = table_config['endpoint']
        id_table = table_config['id']
        type_file = table_config['type_file']


        if (table_config['changed_date'] != None) & (table_config['changed_range'] != None):
            changed_range = table_config['changed_range'].split(" ")
            if changed_range[1] == 'days':
                changed_date = export_date - pydt.timedelta(days = int(changed_range[0]))
            elif changed_range[1] == 'Months':
                changed_date = export_date - relativedelta(months = int(changed_range[0]))
        else:
            changed_date = export_date - pydt.timedelta(days = 1)


        if table == 'eur_ptax':
            start_date = changed_date.strftime("%m-%d-%Y")
            end_date = reference_date.strftime("%m-%d-%Y")

            print('INFO - {} - Getting started extract table {}. Start Date {} and End Date {}'.format(str(pydt.datetime.now()), table, start_date, end_date))

            source_name = endpoint.format(incial = start_date, final = end_date, format = type_file)
            print(source_name)

        else:
            start_date = changed_date.strftime("%d/%m/%Y")
            end_date = reference_date.strftime("%d/%m/%Y")

            print('INFO - {} - Getting started extract table {}. Start Date {} and End Date {}'.format(str(pydt.datetime.now()), table, start_date, end_date))

            source_name = endpoint.format(id = id_table, type_file = type_file)

            params = {
                    "formato": type_file,
                    "dataInicial": start_date,  # Substitua pela data de início desejada
                    "dataFinal": end_date  # Substitua pela data de fim desejada
                }


        url = "{host}{endpoint}".format(host = self.bcb_host, endpoint = source_name)
        print('INFO - {} - Using Url: {}'.format(str(pydt.datetime.now()), url))

        # create local temp file to store downloaded data
        temp_data_file_name = self.generate_temp_file_name()
        dbfs_folder = '/tmp'
        dbfs_file_path = dbfs_folder + temp_data_file_name
        full_file_path = '/dbfs' + dbfs_file_path
        print('INFO - {} - Created local temporary file {}'.format(str(pydt.datetime.now()), full_file_path))

        # Fetch!
        print('INFO - {} - Downloading BCB API Data {}'.format(str(pydt.datetime.now()), source_name))

        if table == 'eur_ptax':
            response = requests.get(url = url)
            data = response.json()['value']
        else:
            response = requests.get(url = url, params=params)
            data = response.json()

        with open(full_file_path , 'w') as file:
            for row in data:
                row_string = json.dumps(row, default = str)
                file.write(row_string+"\n")

        print('INFO - {} - Finished downloading BCB API Data {} to {}'.format(str(pydt.datetime.now()), source_name, full_file_path))

        schema_fields = []
        for field in table_config['schema']:
            if field['type'] == 'STRING':
                 schema_fields.append(StructField(field['field'], StringType()))
            elif field['type'] == 'INT':
                schema_fields.append(StructField(field['field'], IntegerType()))
            elif field['type'] == 'DOUBLE':
                schema_fields.append(StructField(field['field'], DoubleType()))
            elif field['type'] == 'TIMESTAMP':
                schema_fields.append(StructField(field['field'], TimestampType()))
            elif field['type'] == 'DATE':
                schema_fields.append(StructField(field['field'], DateType()))
            elif field['type'] == 'BOOLEAN':
                schema_fields.append(StructField(field['field'], BooeleanType()))
            else:
                schema_fields.append(StructField(field['field'], StringType()))

        df_schema = StructType(schema_fields)
        print(df_schema)

        print('INFO - {} - Creating DataFrame reading data from BCB API response json'.format(str(pydt.datetime.now())))
        if table == 'eur_ptax':
            df = spark.read.json(dbfs_file_path)
        else:
            df = spark.read.json(dbfs_file_path)
            df = df.withColumn("data", to_date(col("data"), "dd/MM/yyyy"))

        print('INFO - {} - Finished BCB API endpoint extraction: {} '.format(str(pydt.datetime.now()), table))
#         #dataframe, dbfs_path, original_source_name
        return df, dbfs_file_path, table + '.csv'
