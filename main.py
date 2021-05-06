# Be sure to run `pip install -r requirements.txt`
# before running this script!
import time
import math

import pandas as pd

from google.cloud import bigquery

from helperutils import HelperUtils
from jira import JiraLoader, JiraData
from intercom import IntercomLoader, IntercomData
from bigquery import BigQueryHelper
from hubspot import HubspotLoader, HubspotData


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


def get_intercom_companies_dataframe(intercom_config):
    page = 1
    intercom_loader = IntercomLoader()
    intercom_data = IntercomData()

    # Load first batch of companies
    raw_companies = intercom_loader.get_companies(page)

    # Start paginated parsing and loading
    while page < intercom_loader.total_pages_for_companies:

        # Iterate over each company item
        for raw_company in raw_companies:

            # Iterate over each tag within a company
            for raw_tag in raw_company.tags[intercom_config['tag_id']]:
                # Add company to intercom_data collection
                intercom_data.add_company(raw_company, raw_tag)

        # Move to next page
        page += 1

        # Load next batch of companies
        raw_companies = intercom_loader.get_companies(page)

    return pd.DataFrame(intercom_data.get_companies())


def get_intercom_contacts_dataframe(intercom_config):
    intercom_loader = IntercomLoader()
    intercom_data = IntercomData()

    # Start paginated parsing and loading
    while True:

        raw_contacts = intercom_loader.get_contacts()
        # Iterate over each company item
        for raw_contact in raw_contacts:

            intercom_data.add_contact(raw_contact)

        # Check if there are more pages
        if intercom_loader.has_more_contacts() != True:
            break

    return pd.DataFrame(intercom_data.get_contacts())


def get_intercom_contact_companies_dataframe(intercom_config):
    intercom_loader = IntercomLoader()
    intercom_data = IntercomData()

    # Start paginated parsing and loading
    while True:
        raw_contacts = intercom_loader.get_contacts()

        # Iterate over each contact item
        for raw_contact in raw_contacts:
            intercom_data.add_contact(raw_contact)

            # Iterate over each company within a contact
            company_count = raw_contact.companies.total_count
            if company_count != 0:
                # Check if total companies are 10 or less.
                if company_count <= 10:
                    for raw_contact_company in raw_contact.companies.get('data'):
                        # Add company to intercom_data collection
                        intercom_data.add_contact_company(
                            raw_contact, raw_contact_company)
                else:

                    raw_contact_companies = intercom_loader.get_contact_companies(
                        raw_contact.id)
                    for raw_contact_company in raw_contact_companies:
                        # Add contact_id and company_id to intercom_data collection
                        intercom_data.add_contact_company(
                            raw_contact, raw_contact_company)

        if intercom_loader.has_more_contacts() != True:
            break

    return pd.DataFrame(intercom_data.get_contact_companies())


def get_hubspot_company_deals_dataframe(hubspot_config):

    bq_helper = BigQueryHelper()
    hubspot_loader = HubspotLoader()
    hubspot_data = HubspotData()

    query_string = hubspot_config['query_string']
    properties = hubspot_config['properties']

    dataframe = bq_helper.get_dataframe_from_query(query_string)

    company_list = list(dataframe['company_id'])
    #company_list = [5396871923]

    for company_id in company_list:
        raw_company_deals = hubspot_loader.get_company_deals(
            company_id, properties)

        for raw_company_deal in raw_company_deals:
            hubspot_data.add_company_deal(company_id, raw_company_deal)

    return pd.DataFrame(hubspot_data.get_company_deals())


def load_jira_dataframe(bq_config, dataframe):
    bq_helper = BigQueryHelper(bq_config['jira_table_id'], bigquery.LoadJobConfig(
        write_disposition=bq_config['jira_write_deposition']))

    bq_helper.load_table(dataframe)
    table = bq_helper.get_table()
    print(
        f"Jira => Loaded {table.num_rows} rows with {len(table.schema)} columns to {bq_helper.table_id}"
    )
    bq_helper.close_client()


def load_intercom_companies_dataframe(bq_config, dataframe):
    bq_helper = BigQueryHelper(
        bq_config['intercom_company_table_id'], bigquery.LoadJobConfig(schema=[
            # Specify the type of columns whose type cannot be auto-detected.
            # For example the "company_id" column uses pandas dtype "object", so its
            # data type is ambiguous.
            bigquery.SchemaField(
                "company_id", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField(
                "last_request_at", bigquery.enums.SqlTypeNames.INT64),
            bigquery.SchemaField(
                "id", bigquery.enums.SqlTypeNames.STRING)
        ],
            write_disposition=bq_config['intercom_company_write_deposition']))

    bq_helper.load_table(dataframe)
    table = bq_helper.get_table()
    print(
        f"Intercom Companies => Loaded {table.num_rows} rows with {len(table.schema)} columns to {bq_helper.table_id}"
    )
    bq_helper.close_client()


def load_intercom_contact_dataframe(bq_config, dataframe):
    bq_helper = BigQueryHelper(
        bq_config['intercom_contact_table_id'], bigquery.LoadJobConfig(schema=[
            # Specify the type of columns whose type cannot be auto-detected.
            # For example the "contact_id" column uses pandas dtype "object", so its
            # data type is ambiguous.
            bigquery.SchemaField(
                "contact_id", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField(
                "external_id", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField(
                "name", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField(
                "type", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField(
                "last_seen_at", bigquery.enums.SqlTypeNames.INT64),
            bigquery.SchemaField(
                "signed_up_at", bigquery.enums.SqlTypeNames.INT64),
            bigquery.SchemaField(
                "city", bigquery.enums.SqlTypeNames.STRING)
        ],
            write_disposition=bq_config['intercom_contact_write_deposition']))

    bq_helper.load_table(dataframe)
    table = bq_helper.get_table()
    print(
        f"Intercom Contact => Loaded {table.num_rows} rows with {len(table.schema)} columns to {bq_helper.table_id}"
    )
    bq_helper.close_client()


def load_intercom_contact_companies_dataframe(bq_config, dataframe):
    bq_helper = BigQueryHelper(
        bq_config['intercom_xref_table_id'], bigquery.LoadJobConfig(schema=[
            # Specify the type of columns whose type cannot be auto-detected.
            # For example the "company_id" column uses pandas dtype "object", so its
            # data type is ambiguous.
            bigquery.SchemaField(
                "contact_id", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField(
                "company_id", bigquery.enums.SqlTypeNames.STRING)
        ],
            write_disposition=bq_config['intercom_xref_write_deposition']))

    bq_helper.load_table(dataframe)
    table = bq_helper.get_table()
    print(
        f"Intercom Xref_Contact_Companies => Loaded {table.num_rows} rows with {len(table.schema)} columns to {bq_helper.table_id}"
    )
    bq_helper.close_client()


def load_hubspot_company_deals_dataframe(bq_config, dataframe):
    bq_helper = BigQueryHelper(
        bq_config['hubspot_company_deals_table_id'], bigquery.LoadJobConfig(

            write_disposition=bq_config['hubspot_company_deals_write_deposition']))

    bq_helper.load_table(dataframe)
    table = bq_helper.get_table()
    print(
        f"Hubspot Company_Deals => Loaded {table.num_rows} rows with {len(table.schema)} columns to {bq_helper.table_id}"
    )
    bq_helper.close_client()


def main():
    utils = HelperUtils()
    bq_config = utils.get_bigquery_config()

    start_time = time.time()

    # Load Jira dataframe into BigQuery
    print("Jira import and load started")
    jira_dataframe = get_jira_dataframe(utils.get_jira_config())
    load_jira_dataframe(bq_config, jira_dataframe)
    print("Jira import and load completed")

    # Load Intercom Companies dataframe into BigQuery
    print("Intercom import and load started")
    intercom_companies_dataframe = get_intercom_companies_dataframe(
        utils.get_intercom_config())
    load_intercom_companies_dataframe(bq_config, intercom_companies_dataframe)

    # Load Intercom contacts dataframe into BigQuery
    intercom_contacts_dataframe = get_intercom_contacts_dataframe(
        utils.get_intercom_config())
    load_intercom_contact_dataframe(bq_config, intercom_contacts_dataframe)

    # Load Intercom contact companies dataframe into BigQuery
    intercom_contact_companies_dataframe = get_intercom_contact_companies_dataframe(
        utils.get_intercom_config())
    load_intercom_contact_companies_dataframe(
        bq_config, intercom_contact_companies_dataframe)
    print("Intercom import and load completed")

    # Load Hubspot company deals dataframe into BigQuery
    print("Hubspot import and load started")
    hubspot_company_deals_dataframe = get_hubspot_company_deals_dataframe(
        utils.get_hubspot_config())

    load_hubspot_company_deals_dataframe(
        bq_config, hubspot_company_deals_dataframe)
    print("Hubspot import and load completed")

    print(f"Completed in {math.ceil(time.time() - start_time)} seconds")


# Call Main Function
if __name__ == "__main__":
    main()
