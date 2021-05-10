from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud import bigquery_storage

from helperutils import HelperUtils


class BigQueryHelper:
    def __init__(self, table_id=None, job_config=None):
        utils = HelperUtils()
        bigquery_config = utils.get_bigquery_config()

        self.has_table_loaded = False
        self.credentials = service_account.Credentials.from_service_account_file(
            rf"{bigquery_config['credentials']}")
        self.client = bigquery.Client(
            credentials=self.credentials, project=bigquery_config['project_id'])
        self.storage_client = bigquery_storage.BigQueryReadClient(
            credentials=self.credentials)

        self.table_id = table_id
        self.job_config = job_config

    def load_table(self, dataframe):
        self.job = self.client.load_table_from_dataframe(
            dataframe, self.table_id, job_config=self.job_config)
        self.job.result()

    def get_table(self):
        if (self.job != None and self.job.done()):
            return self.client.get_table(self.table_id)
        else:
            return None

    def get_dataframe_from_query(self, query):
        return self.client.query(query).result().to_dataframe(self.storage_client)

    def close_client(self):
        self.client.close()
