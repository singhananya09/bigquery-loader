from google.cloud import bigquery
from google.oauth2 import service_account

from helperutils import HelperUtils


class BigQueryHelper:
    def __init__(self, table_id, job_config):
        utils = HelperUtils()
        bigquery_config = utils.get_bigquery_config()

        self.has_table_loaded = False
        self.credentials = service_account.Credentials.from_service_account_file(
            rf"{bigquery_config['credentials']}")
        self.client = bigquery.Client(
            credentials=self.credentials, project=bigquery_config['project_id'])

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

    def close_client(self):
        self.client.close()
