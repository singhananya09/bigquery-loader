import json
from datetime import datetime

import requests as rq
from dotmap import DotMap

from helperutils import HelperUtils


class HubspotLoader:
    def __init__(self):
        utils = HelperUtils()
        self.hubspot_config = utils.get_hubspot_config()
        self.auth_token = utils.get_hubspot_token()
        self.host = f"{self.hubspot_config['host']}"
        self.endpoint = f"{self.hubspot_config['contact_deals_endpoint']}"
        self.association = f"{self.hubspot_config['association']}"
        self.limit = f"{self.hubspot_config['limit']}"

    def get_company_deals(self, company_id, properties):
        query_params = [
            f"includeAssociations={self.association}",
            f"properties={properties}",
            f"limit={self.limit}",
            f"hapikey={self.auth_token}"
        ]
        request_url = f"{self.host}{self.endpoint}/{company_id}/paged?{'&'.join(query_params)}"
        print(f"GET {request_url}")
        response = rq.get(request_url)
        response_body = DotMap(json.loads(response.text))

        return response_body.deals


class HubspotData:
    def __init__(self):
        self.company_deals = []

    def add_company_deal(self, company_id, raw_company_deal):
        parsed_company_deal = {
            'company_id': company_id,
            'deal_id': raw_company_deal.dealId,
            'timestamp': raw_company_deal.properties.dealname.timestamp,
            'record_ins_timestamp': datetime.now()
        }
        self.company_deals.append(parsed_company_deal)

    def get_company_deals(self):
        return self.company_deals
