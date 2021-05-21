import json
from time import time, sleep

import aiohttp
import asyncio
import requests as rq
from dotmap import DotMap

from helperutils import HelperUtils
from utils.networkutils import NetworkUtils


class HubspotLoader:
    def __init__(self):
        utils = HelperUtils()
        self.networkutils = NetworkUtils()
        self.hubspot_config = utils.get_hubspot_config()
        self.auth_token = utils.get_hubspot_token()
        self.host = f"{self.hubspot_config['host']}"
        self.endpoint = f"{self.hubspot_config['contact_deals_endpoint']}"
        self.association = f"{self.hubspot_config['association']}"
        self.limit = f"{self.hubspot_config['limit']}"
        self.company_deals_urls = []
        self.requests_per_second = self.hubspot_config['requests_per_second']

    def add_company_deals_urls(self, company_id, properties):
        query_params = [
            f"includeAssociations={self.association}",
            f"properties={properties}",
            f"limit={self.limit}",
            f"hapikey={self.auth_token}"
        ]
        request_url = f"{self.host}{self.endpoint}/{company_id}/paged?{'&'.join(query_params)}"

        self.company_deals_urls.append(request_url)

    def get_company_deals_async(self):
        return self.networkutils.get_response(self.company_deals_urls, self.requests_per_second)

    def get_company_deals(self, request_url):
        response = rq.get(request_url)
        response_body = DotMap(json.loads(response.text))
        return response_body.deals

    def get_company_deal_urls(self):
        return self.company_deals_urls
