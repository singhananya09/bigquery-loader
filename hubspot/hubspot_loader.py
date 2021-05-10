import json
from time import time, sleep

import aiohttp
import asyncio
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
        self.company_deals_urls = []

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
        utils = HelperUtils()
        responses = []

        async def get_company_deal(session, url):
            async with session.get(url) as res:
                while True:
                    try:
                        print(f"GET {url}")
                        raw_response = await res.read()
                        response = DotMap(json.loads(raw_response))
                        return response
                    except Exception as e:
                        print(e)
                        sleep(1)

        async def get_company_deals_for_batch(batch):
            async with aiohttp.ClientSession() as session:
                tasks = []
                for url in batch:
                    tasks.append(asyncio.ensure_future(
                        get_company_deal(session, url)))
                original_response = await asyncio.gather(*tasks)
                responses.extend(original_response)

        batches = utils.get_list_of_batches(self.company_deals_urls,
                                            self.hubspot_config['requests_per_second'])
        count = 1
        for batch in batches:
            print(f"Get Batch => {count}")
            asyncio.run(get_company_deals_for_batch(batch))
            sleep(1)
            count += 1

        return responses

    def get_company_deals(self, request_url):
        response = rq.get(request_url)
        response_body = DotMap(json.loads(response.text))
        return response_body.deals

    def get_company_deal_urls(self):
        return self.company_deals_urls
