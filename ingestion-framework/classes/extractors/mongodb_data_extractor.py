class MongoDBDataExtractor(DataExtractor):

    def __init__(self, sm_user_secret_id, sm_pwd_secret_id, _database, _instance, _service, _type):

        self.sm_client = secretmanager.SecretManagerServiceClient()

        self.sm_user  = self.sm_client.access_secret_version(request={"name": "projects/data-prod-295413/secrets/{}/versions/1".format(sm_user_secret_id)})\
            .payload.data.decode("UTF-8")
        self.sm_pwd   = self.sm_client.access_secret_version(request={"name": "projects/data-prod-295413/secrets/{}/versions/1".format(sm_pwd_secret_id)})\
            .payload.data.decode("UTF-8")

        self._database = _database
        self._instance = _instance
        self._servive = _service
        self._type = _type

        self.connection = ("{}+srv://{}:{}@{}.a4flz.{}.net/{}").format(self._servive, self.sm_user, self.sm_pwd, self._instance, self._type, self._database)

    def extract(self, export_date, reference_date, table, table_config):

        extract_mode = table_config['extract_mode']
        collection = table_config['endpoint']
        date_fields = table_config['date_fields']
        string_fields = table_config['string_fields']
        double_fields = table_config['double_fields']
        binary_fields = table_config['binary_fields']

        print("INFO - {} - Starting AccountMoviment table".format(str(pydt.datetime.now())))

        result = spark.read\
            .format("com.mongodb.spark.sql.DefaultSource")\
            .option("uri", self.connection) \
            .option("database", self._database)\
            .option("collection", collection)\
            .load()

        explode = result.select(result.CustomerId, result._id, posexplode(result.AccountMoviment).alias('pos','AccountMoviment'))

        columns = explode.select(to_json("AccountMoviment").alias('fields')).limit(1)

        list_fields = []
        select_columns = []

        for row in columns.collect():
            for item in eval(row.fields):
                list_fields.append(item)

        select_columns.append(col(f"CustomerId.data").alias('CustomerId'))
        select_columns.append(col(f"_id.data").alias('id'))

        for key in list_fields:
            if key == '_id':
                column_name = 'MovimentId'
                select_columns.append(col(f"AccountMoviment.{key}.data").alias(column_name))
            else:
                column_name = key
                select_columns.append(col(f"AccountMoviment.{key}").alias(column_name))

        df_explode = explode.select(select_columns)

        print("INFO - {} - Creating Dataframe explode".format(str(pydt.datetime.now())))

        for f in df_explode.schema.fields:
            if str(f.name) in date_fields:
                field = f.name
                df_explode = df_explode.withColumn(field,sqlfunc.when(sqlfunc.col(field) == sqlfunc.lit('0001-01-01'),sqlfunc.lit(None)).otherwise(sqlfunc.col(field)))
            elif str(f.name) in string_fields:
                field = f.name
                df_explode = df_explode.withColumn(field, sqlfunc.col(field).cast(sqltypes.StringType()))
            elif str(f.name) in double_fields:
                field = f.name
                df_explode = df_explode.withColumn(field, sqlfunc.col(field).cast(sqltypes.DoubleType()))
            elif str(f.name) in binary_fields:
                field = f.name
                df_explode = df_explode.withColumn(field, sqlfunc.base64(sqlfunc.col(field)))

        if (table_config['changed_date'] != None) & (table_config['changed_range'] != None):
            changed_range = table_config['changed_range'].split(" ")

            print("INFO - Filtering last {} {}".format(changed_range[0], changed_range[1]))

            startdate = export_date - pydt.timedelta(days = int(changed_range[0]))
            endDate = reference_date

            df = df_explode.filter(df_explode['PositionDate'] >= sqlfunc.lit(startdate)) \
                    .filter(df_explode['PositionDate'] <= sqlfunc.lit(endDate))

            print('INFO - {} - Finished MongoDB table extraction: {} '.format(str(pydt.datetime.now()), table))
        else:
            df = df_explode
            print('INFO - {} - Finished MongoDB table extraction: {} '.format(str(pydt.datetime.now()), table))

        #dataframe, dbfs_path, original_source_name
        return df, None, None
