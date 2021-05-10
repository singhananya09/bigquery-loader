from math import ceil
from time import time

import pandas as pd
from google.cloud import bigquery

from jira.jira_loader import JiraLoader
from jira.jira_data import JiraData
from helperutils import HelperUtils
from bigquery import BigQueryHelper


def get_jira_dataframe(jira_config):
    start_at = 0
    jira_loader = JiraLoader()
    jira_data = JiraData()

    # Load first batch of issues
    raw_issues = jira_loader.get_issues(start_at)

    # Start paginated parsing and loading
    while len(raw_issues) != 0:

        # Iterate over each issue item
        for raw_issue in raw_issues:

            # Iterate over each sprint within an issue
            for raw_sprint in raw_issue.fields[jira_config['sprint_id']]:
                # Add issue to jira_data collection
                jira_data.add_issue(raw_issue, raw_sprint)

        # Move start_at to next page
        start_at += len(raw_issues)

        # Load next batch of issues
        raw_issues = jira_loader.get_issues(start_at)

    return pd.DataFrame(jira_data.get_issues())


def load_jira_dataframe(bq_config, dataframe):
    bq_helper = BigQueryHelper(bq_config['jira_table_id'], bigquery.LoadJobConfig(
        write_disposition=bq_config['jira_write_deposition']))

    bq_helper.load_table(dataframe)
    table = bq_helper.get_table()
    print(
        f"Jira => Loaded {table.num_rows} rows with {len(table.schema)} columns to {bq_helper.table_id}"
    )
    bq_helper.close_client()


def jira_init():
    utils = HelperUtils()
    bq_config = utils.get_bigquery_config()
    start_time = time()

    # Load Jira dataframe into BigQuery
    print("Jira import and load started")
    jira_dataframe = get_jira_dataframe(utils.get_jira_config())
    load_jira_dataframe(bq_config, jira_dataframe)

    print(f"Jira => Completed in {ceil(time() - start_time)} seconds")
