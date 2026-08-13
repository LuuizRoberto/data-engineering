class GalileoDataExtractor(DataExtractor):

    def __init__(self, _gcs_bucket, _projetct_id, sm_pwd_secret_id, sm_private_key):

        self.sm_client = secretmanager.SecretManagerServiceClient()

        self.gcs_bucket = _gcs_bucket
        self.storage_client = storage.Client('data-prod-295413')

        self.sm_passphrase = self.sm_client.access_secret_version(request={"name": "projects/data-prod-295413/secrets/{}/versions/2".format(sm_pwd_secret_id)})\
            .payload.data.decode("UTF-8")

        self.sm_private_key = self.sm_client.access_secret_version(request={"name": "projects/data-prod-295413/secrets/{}/versions/3".format(sm_private_key)})\
            .payload.data.decode("UTF-8")

    def import_private_key(self, key_path, gpg):
        with open(key_path, "rb") as f:
            key_data = f.read()
        result = gpg.import_keys(key_data)

        return result

    def extract(self, export_date, reference_date, table, table_config):

        extract_mode = table_config['extract_mode']
        endpoint = table_config['endpoint']

        source_blob_name = endpoint.format(dateint = export_date.strftime("%Y%m%d"))

        private_key_file = '/tmp/private_key_galileo.asc'

        with open(private_key_file, 'w') as private_key_galileo:
            private_key_galileo.write(self.sm_private_key)

        temp_data_file_name_input = self.generate_temp_file_name()
        dbfs_folder_input = '/tmp/'
        dbfs_file_path_input = dbfs_folder_input + temp_data_file_name_input
        full_file_path_input = '/dbfs' + dbfs_file_path_input

        print('INFO - {} - Created local temporary file encrypted data {}'.format(str(pydt.datetime.now()), full_file_path_input))

        temp_data_file_name = self.generate_temp_file_name()
        dbfs_folder = '/tmp/'
        dbfs_file_path = dbfs_folder + temp_data_file_name
        full_file_path = '/dbfs' + dbfs_file_path

        print('INFO - {} - Created local temporary file decrypted data {}'.format(str(pydt.datetime.now()), full_file_path))

        # Fetch!
        print('INFO - {} - Downloading GCS object {}'.format(str(pydt.datetime.now()), source_blob_name))

        bucket = self.storage_client.bucket(self.gcs_bucket)

        blob = bucket.blob(source_blob_name)
        blob.download_to_filename(full_file_path_input)
        print('INFO - {} - Finished downloading GCS object {} to {}'.format(str(pydt.datetime.now()), source_blob_name, full_file_path_input))

        gpg = gnupg.GPG()

        self.import_private_key(private_key_file, gpg)

        print('INFO - {} - Import private key for decryped file data {}'.format(str(pydt.datetime.now()), full_file_path_input))

        with open(full_file_path_input, "rb") as f:
            encrypted_data = f.read()
            data = gpg.decrypt(encrypted_data, passphrase = self.sm_passphrase)
            print(data)
            decrypted_data = data.data
            with open(full_file_path, "wb") as out_file:
                out_file.write(decrypted_data)

        if ('replace_header' in table_config) and table_config['replace_header'] == True:
            with open(full_file_path, 'r') as original:
                lines = original.readlines()
                if len(lines) == 0:
                    lines.append(table_config['delimiter'].join([f['field'] for f in table_config['schema']]) +"\n")
                else:
                    lines[0] = table_config['delimiter'].join([f['field'] for f in table_config['schema']]) +"\n"

            with open(full_file_path, 'w') as added_header: added_header.writelines(lines)

        print('INFO - {} - Downloading encrypted file data to file {}'.format(str(pydt.datetime.now()), full_file_path))

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
            else:
                schema_fields.append(StructField(field['field'], StringType()))

        csv_schema = StructType(schema_fields)

        print('INFO - {} - Finished created schema: {} '.format(str(pydt.datetime.now()), csv_schema))

        print('INFO - {} - Creating DataFrame reading data from DBFS'.format(str(pydt.datetime.now())))
        df = spark.read.format("csv") \
                .options(header="True", delimiter=table_config['delimiter']) \
                .schema(csv_schema) \
                .load(dbfs_file_path)

        df = df.dropna(how='all')
        df = df.filter(~col(df.columns[0]).contains("TTRAILER"))

        if ('exclude_columns' in table_config):
            if (len(table_config['exclude_columns']) > 0):
                for item in table_config['exclude_columns']:
                    df = df.drop(item)

        print('INFO - {} - Finished GCS table extraction: {} '.format(str(pydt.datetime.now()), table))

        #dataframe, dbfs_path, original_source_name
        return df, dbfs_file_path, source_blob_name
