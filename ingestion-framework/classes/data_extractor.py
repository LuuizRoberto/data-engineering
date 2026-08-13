class DataExtractor(ABC):

    @abstractmethod
    def extract(self, export_date, reference_date, table, table_config):
        pass

    def copy_to_dbfs(self, local_file_name, dbfs_path):
        local_file = open(local_file_name, 'r')
        print('INFO - {} - Copying local file {} to DBFS: {}'.format(str(pydt.datetime.now()), local_file_name, dbfs_path))
        dbutils.fs.put(dbfs_path, local_file.read(), overwrite = True)
        local_file.close()
        print('INFO - {} - Finished copying local file {} to DBFS: {}'.format(str(pydt.datetime.now()), local_file_name, dbfs_path))

        return dbfs_path

    def generate_temp_file_name(self):
         # create local temp file to store downloaded data
        temp_file = tempfile.NamedTemporaryFile(mode='w+', encoding='utf8', newline='\n', dir='/')
        temp_file_name = temp_file.name
        temp_file.close()

        return temp_file_name

    def strip_accents(self, s):
        return ''.join(c for c in unicodedata.normalize('NFD', s)
                  if unicodedata.category(c) != 'Mn')
