class Pipeline(ABC):

    def __init__(self, _dependencies = []):
        self.dependencies = _dependencies
        self.storage_client = storage.Client(project='data-prod-295413')

    @abstractmethod
    def run(self):
        pass

    @staticmethod
    def upload_blob(storage_client, bucket_name, source_file_name, destination_blob_name):
        """Uploads a file to the bucket."""
        # bucket_name = "your-bucket-name"
        # source_file_name = "local/path/to/file"
        # destination_blob_name = "storage-object-name"

        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)

        blob.upload_from_filename(source_file_name)

        print(
            "File {} uploaded to path: gs://{}/{}.".format(
                source_file_name, bucket_name, destination_blob_name
            )
        )

    def strip_accents(self, s):
        return ''.join(c for c in unicodedata.normalize('NFD', s)
                  if unicodedata.category(c) != 'Mn')
