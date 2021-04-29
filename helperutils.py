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

    def get_bigquery_config(self):
        with open(self.config_file, "r") as f:
            return yml.safe_load(f)['big_query']

    def get_jira_auth_username(self):
        return os.environ['JIRA_USERNAME']

    def get_jira_auth_token(self):
        return os.environ['JIRA_TOKEN']

    def get_intercom_token(self):
        return os.environ['INTERCOM_TOKEN']
