# Be sure to run `pip install -r requirements.txt`
# before running this script!
import pandas as pd

from google.cloud import bigquery

from helperutils import HelperUtils
from jira import JiraLoader, JiraData
from intercom import IntercomLoader, IntercomData
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
                
                if company_count <= 10:
                    

                    for raw_contact_company in raw_contact.companies.get('data'):
                        # Add company to intercom_data collection
                        intercom_data.add_contact_companies(raw_contact, raw_contact_company)
                        
                if company_count > 10:
                    raw_contact_companies = intercom_loader.get_contact_companies(raw_contact.id)
                    
                    for raw_contact_company in raw_contact_companies:
                        # Add contact_id and company_id to intercom_data collection
                        intercom_data.add_contact_companies(raw_contact, raw_contact_company)
        
        if intercom_loader.has_more_contacts() != True:
            break


    print(pd.DataFrame(intercom_data.get_contact_companies()))


def load_jira_dataframe(bq_config, dataframe):
    bq_helper = BigQueryHelper(bq_config['jira_table_id'], bigquery.LoadJobConfig(
        write_disposition=bq_config['write_deposition']))

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
            bigquery.SchemaField("id", bigquery.enums.SqlTypeNames.STRING)
        ],
            write_disposition=bq_config['write_deposition']))

    bq_helper.load_table(dataframe)
    table = bq_helper.get_table()
    print(
        f"Intercom Companies => Loaded {table.num_rows} rows with {len(table.schema)} columns to {bq_helper.table_id}"
    )
    bq_helper.close_client()


def main():
    utils = HelperUtils()
    bq_config = utils.get_bigquery_config()

    '''
    # Load Jira dataframe into BigQuery
    jira_dataframe = get_jira_dataframe(utils.get_jira_config())
    load_jira_dataframe(bq_config, jira_dataframe)

    # Load Intercom Companies dataframe into BigQuery
    intercom_companies_dataframe = get_intercom_companies_dataframe(
        utils.get_intercom_config())
    load_intercom_companies_dataframe(bq_config, intercom_companies_dataframe)
    
    # Load Intercom contacts dataframe into BigQuery
    intercom_contacts_dataframe = get_intercom_contacts_dataframe(
    utils.get_intercom_config())
    print(intercom_contacts_dataframe)

    '''
    # Load Intercom contacts dataframe into BigQuery
    intercom_contact_companies_dataframe = get_intercom_contact_companies_dataframe(
    utils.get_intercom_config())
    print(intercom_contact_companies_dataframe)

# Call Main Function
if __name__ == "__main__":
    main()
