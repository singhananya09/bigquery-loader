from math import ceil
from time import time
import json
from dotmap import DotMap

import pandas as pd
from google.cloud import bigquery

from hubspot.hubspot_loader import HubspotLoader
from hubspot.hubspot_data import HubspotData
from helperutils import HelperUtils
from bigquery import BigQueryHelper


def get_hubspot_company_deals_dataframe(hubspot_config):
    bq_helper = BigQueryHelper()
    hubspot_loader = HubspotLoader()
    hubspot_data = HubspotData()

    query_string = hubspot_config['query_string']
    properties = hubspot_config['properties']

    dataframe = bq_helper.get_dataframe_from_query(query_string)

    company_list = list(dataframe['company_id'])

    for company_id in company_list:
        hubspot_loader.add_company_deals_urls(company_id, properties)

    raw_company_deals = hubspot_loader.get_company_deals_async()

    for raw_company_deal in raw_company_deals:
        if len(raw_company_deal.deals) == 0:
            continue

        for raw_deal in raw_company_deal.deals:
            hubspot_data.add_company_deal(raw_deal)

    return pd.DataFrame(hubspot_data.get_company_deals())


def load_hubspot_company_deals_dataframe(bq_config, dataframe):
    bq_helper = BigQueryHelper(
        bq_config['hubspot_company_deals_table_id'], bigquery.LoadJobConfig(schema=[
            # Specify the type of columns whose type cannot be auto-detected.
            # For example the "company_id" column uses pandas dtype "object", so its
            # data type is ambiguous.
            bigquery.SchemaField(
                "company_id", bigquery.enums.SqlTypeNames.INT64),
            bigquery.SchemaField(
                "deal_id", bigquery.enums.SqlTypeNames.INT64)],

            write_disposition=bq_config['hubspot_company_deals_write_deposition']))

    bq_helper.load_table(dataframe)
    table = bq_helper.get_table()
    print(
        f"Hubspot Company_Deals => Loaded {table.num_rows} rows with {len(table.schema)} columns to {bq_helper.table_id}"
    )
    bq_helper.close_client()


def hubspot_init():
    utils = HelperUtils()
    bq_config = utils.get_bigquery_config()
    start_time = time()

    # Load Hubspot company deals dataframe into BigQuery

    print("Hubspot import and load started")
    hubspot_company_deals_dataframe = get_hubspot_company_deals_dataframe(
        utils.get_hubspot_config())

    load_hubspot_company_deals_dataframe(
        bq_config, hubspot_company_deals_dataframe)

    print(f"Hubspot => Completed in {ceil(time() - start_time)} seconds")
