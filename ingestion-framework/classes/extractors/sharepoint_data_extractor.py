class SharepointDataExtractor(DataExtractor):

    def __init__(self, sm_user_secret_id, sm_pwd_secret_id, _site_url):

        self.sm_client = secretmanager.SecretManagerServiceClient()

        sm_user  = self.sm_client.access_secret_version(request={"name": "projects/data-prod-295413/secrets/{}/versions/1".format(sm_user_secret_id)})\
            .payload.data.decode("UTF-8")
        sm_pwd   = self.sm_client.access_secret_version(request={"name": "projects/data-prod-295413/secrets/{}/versions/1".format(sm_pwd_secret_id)})\
            .payload.data.decode("UTF-8")

        client_credentials = ClientCredential('{client_id}'.format(client_id = sm_user),
                                      '{client_secret}'.format(client_secret = sm_pwd))

        self.ctx = ClientContext(_site_url).with_credentials(client_credentials)

    def detect_file_encoding(self,filepath):
        """Detects the encoding of a given file."""
        with open(filepath, 'rb') as f:
            # Read a sufficient amount of data to allow chardet to analyze
            raw_data = f.read(100000)  # Read up to 100KB for better accuracy
            result = chardet.detect(raw_data)
            return result['encoding']
        f.close()

    def extract(self, export_date, reference_date, table, table_config):

        extract_mode = table_config['extract_mode']
        endpoint = table_config['endpoint']
        ds_type = table_config['ds_type']
        date_format = table_config['filename_date_format']

        export_year = export_date.year
        export_month = export_date.month

        if export_date.month <=9:
            export_month = "0"+"{}".format(export_date.month)
        else:
            export_month = export_date.month

        # if table_config['endpoint_type'] == 'file':
        if table == "search_comments":
            source_blob_name = endpoint.format(year = export_year, month = export_month)
        else:
            source_blob_name = endpoint.replace('{'+table_config['filename_date_field']+'}',
                                                 reference_date.strftime(table_config['filename_date_format']))

        # create local temp file to store downloaded data
        temp_data_file_name = self.generate_temp_file_name() + '.{}'.format(table_config['ds_type'])
        dbfs_folder = '/tmp'
        dbfs_file_path = dbfs_folder + temp_data_file_name
        full_file_path = '/dbfs' + dbfs_file_path
        full_file_path_new = '/dbfs' + dbfs_file_path.replace('.csv','_new.csv')

        print('INFO - {} - Created local temporary file {}'.format(str(pydt.datetime.now()), full_file_path))

        # Fetch!
        print('INFO - {} - Downloading Sharepoint object {}'.format(str(pydt.datetime.now()), source_blob_name))

        with open(full_file_path, "wb") as local_file:
            file = self.ctx.web.get_file_by_server_relative_url(source_blob_name).download(local_file).execute_query()

        print('INFO - {} - Finished downloading Sharepoint object {} to {}'.format(str(pydt.datetime.now()), source_blob_name, full_file_path))

        if(ds_type == 'csv'):
            orig_encoding = self.detect_file_encoding(full_file_path)
            if orig_encoding.lower() != 'utf-8-sig':
                try:
                    with open(full_file_path, 'r', encoding=orig_encoding) as infile:
                        content = infile.read()

                    with open(full_file_path_new, 'w', encoding='utf-8-sig') as outfile:
                        outfile.write(content)

                    full_file_path = full_file_path_new
                    dbfs_file_path = dbfs_file_path.replace('.csv','_new.csv')

                    # print(f"File '{full_file_path}' successfully converted to 'utf-8-sig'.")
                    print(f"File '{full_file_path_new}' successfully converted from '{orig_encoding}' to 'utf-8-sig'.")

                except FileNotFoundError:
                    print(f"Error: The file '{full_file_path}' was not found.")
                except UnicodeDecodeError as e:
                    print(f"Error decoding the input file': {e}")
                except Exception as e:
                    print(f"An unexpected error occurred: {e}")

            with open(full_file_path, 'r', encoding='utf-8-sig') as original:
                csv_reader = csv.reader(original, delimiter=table_config['delimiter'])
                prep_file_cols = next(csv_reader)
                file_cols = []
                for col in prep_file_cols:
                    file_cols.append(col.replace('"','').strip())
                num_columns = len(file_cols)

        if(ds_type == 'xlsx'):
            workbook = openpyxl.load_workbook(full_file_path)
            sheet = workbook.active
            # Get the first row (header row) of the worksheet
            header_row = sheet[1]
            # Extract the column names from the header row
            file_cols = [cell.value for cell in header_row]
            num_columns = len(file_cols)
            workbook.close()

        if (num_columns != len(table_config['header'])):
            print('ERROR - {} - Downloaded Sharepoint file {} has {}  columns while configured header has {} columns'.format(str(pydt.datetime.now()),
                                                                                                                    source_blob_name, num_columns, len(table_config['header'])))
            raise Exception('Downloaded Sharepoint file {} has {}  columns while configured header has {} columns'.format(source_blob_name, num_columns, len(table_config['header'])))

        # generate file header from table_config header
        if table_config['has_header'] == False:
            header_line = []
            if(ds_type == 'csv'):
                with open(full_file_path, 'r') as original:
                    lines = original.readlines()
                    header_line.append(table_config['delimiter'].join([f['field'].strip() for f in table_config['header']]) +"\n")
                    file_lines = header_line + lines
                with open(full_file_path, 'w') as added_header: added_header.writelines(file_lines)
        else:
            # validate file header and config header against each other
            header_fields  = [hfield['field'].strip() for hfield in table_config['header']]
            # print('file cols:', file_cols)
            for file_col in file_cols:
                if file_col not in header_fields:
                    print('ERROR - {} - Downloaded Sharepoint file {} has different column names than in configured header'.format(str(pydt.datetime.now()), source_blob_name))
                    print('file col:', file_col)
                    print('headers:',header_fields)
                    raise Exception('Downloaded Sharepoint file {} has different column names than in configured header'.format(source_blob_name))
            for header_field in header_fields:
                if header_field not in file_cols:
                    print('ERROR - {} - Downloaded Sharepoint file {} has different column names than in configured header'.format(str(pydt.datetime.now()), source_blob_name))
                    print('header:', header_field)
                    print('file cols:', file_cols)
                    raise Exception('Downloaded Sharepoint file {} has different column names than in configured header'.format(source_blob_name))
            #add table_config header to empty file
            if(ds_type == 'csv'):
                with open(full_file_path, 'r') as original:
                    lines = original.readlines()
                    if len(lines) == 0:
                        lines.append(table_config['delimiter'].join([f['field'] for f in table_config['header']]) +"\n")
                    else:
                        lines[0] = table_config['delimiter'].join([f['field'] for f in table_config['header']]) +"\n"
                with open(full_file_path, 'w') as added_header: added_header.writelines(lines)

        if(ds_type == 'csv'):
            del csv_reader

        print('INFO - {} - Creating DataFrame reading data from DBFS'.format(str(pydt.datetime.now())))
        schema_fields = []
        for field in table_config['header']:
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
            else:
                schema_fields.append(StructField(field['field'], StringType()))

        df_schema = StructType(schema_fields)
        if(ds_type == 'csv'):
            df = spark.read.format("csv") \
                    .options(header="True", delimiter=table_config['delimiter']) \
                    .schema(df_schema) \
                    .load(dbfs_file_path)

        if(ds_type == 'xlsx'):
            df = spark.read.format("excel") \
                    .option("header", "true") \
                    .schema(df_schema) \
                    .load(dbfs_file_path)

        print('INFO - {} - Finished Sharepoint table extraction: {} '.format(str(pydt.datetime.now()), table))

        #dataframe, dbfs_path, original_source_name
        return df, dbfs_file_path, source_blob_name

    def extract_from_raw(self, raw_path, table_config):
        print('INFO - {} - Creating DataFrame reading data from Raw {}'.format(str(pydt.datetime.now()), raw_path))
        schema_fields = []
        for field in table_config['header']:
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
            else:
                schema_fields.append(StructField(field['field'], StringType()))

        df_schema = StructType(schema_fields)
        if(table_config['ds_type'] == 'csv'):
            df = spark.read.format("csv") \
                    .options(header="True", delimiter=table_config['delimiter']) \
                    .schema(df_schema) \
                    .load(raw_path)
        if(table_config['ds_type'] == 'xlsx'):
            df = spark.read.format("excel") \
                    .option("header", "true") \
                    .schema(df_schema) \
                    .load(raw_path)

        return df
