import json

import requests as rq
from requests.structures import CaseInsensitiveDict
from dotmap import DotMap

from helperutils import HelperUtils


class IntercomLoader:
    def __init__(self):
        utils = HelperUtils()
        headers = CaseInsensitiveDict()
        headers["Accept"] = "application/json"
        headers["Authorization"] = f"Bearer {utils.get_intercom_token()}"

        self.contact_id = None
        self.starting_after = None
        self.total_pages_for_companies = None
        self.intercom_config = utils.get_intercom_config()
        self.request_headers = headers
        self.host = self.intercom_config['host']

    def __get_response_for_endpoint(self, endpoint, page, starting_after, contact_id):
        request_url = f"{self.host}{endpoint}?per_page={self.intercom_config['per_page']}"
        if page != None:
            request_url = f"{request_url}&page={page}"
        if starting_after != None:
            request_url = f"{request_url}&starting_after={starting_after}"
        if contact_id != None:
            request_url = f"{self.host}{endpoint}/{contact_id}/companies"

        print(f"GET {request_url}")
        response = rq.get(request_url, headers=self.request_headers)

        return DotMap(json.loads(response.text))

    def has_more_contacts(self):
        if self.starting_after == None:
            return False
        else:
            return True

    def get_companies(self, page):
        response_body = self.__get_response_for_endpoint(
            self.intercom_config['companies_endpoint'], page, None, None)
        self.total_pages_for_companies = response_body.pages.total_pages

        return response_body.data

    def get_contacts(self):
        response_body = self.__get_response_for_endpoint(
            self.intercom_config['contacts_endpoint'], None, self.starting_after, None)
        self.starting_after = None

        if 'next' in response_body.pages:
            self.starting_after = response_body.pages['next']['starting_after']

        return response_body.data

    def get_contact_companies(self, contact_id):
        response_body = self.__get_response_for_endpoint(
            self.intercom_config['contacts_endpoint'], None, None, contact_id)

        return response_body.data
