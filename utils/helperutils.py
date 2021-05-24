import os

import yaml as yml


class HelperUtils:
    def __init__(self):
        self.config_file = "config.yml"

    def get_jira_config(self):
        with open(self.config_file, "r") as f:
            return yml.safe_load(f)['jira']

    def get_intercom_config(self):
        with open(self.config_file, "r") as f:
            return yml.safe_load(f)['intercom']

    def get_hubspot_config(self):
        with open(self.config_file, "r") as f:
            return yml.safe_load(f)['hubspot']

    def get_bigquery_config(self):
        with open(self.config_file, "r") as f:
            return yml.safe_load(f)['big_query']

    def get_network_config(self):
        with open(self.config_file, "r") as f:
            return yml.safe_load(f)['network']

    def get_jira_auth_username(self):
        return os.environ['JIRA_USERNAME']

    def get_jira_auth_token(self):
        return os.environ['JIRA_TOKEN']

    def get_intercom_token(self):
        return os.environ['INTERCOM_TOKEN']

    def get_hubspot_token(self):
        return os.environ['HUBSPOT_TOKEN']

    def get_list_of_batches(self, source_list, batch_size):
        batches = []
        batch = []
        source_list_len = len(source_list)
        for i in range(source_list_len):
            batch.append(source_list[i])
            if (len(batch) == batch_size or i == source_list_len - 1):
                batches.append(batch)
                batch = []

        return batches
