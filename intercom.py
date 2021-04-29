import json
from datetime import datetime

import requests as rq
from requests.structures import CaseInsensitiveDict
from dotmap import DotMap

from helperutils import HelperUtils
from bigquery import BigQueryHelper


class IntercomLoader:
    def __init__(self):
        utils = HelperUtils()
        headers = CaseInsensitiveDict()
        headers["Accept"] = "application/json"
        headers["Authorization"] = f"Bearer {utils.get_intercom_token()}"

        self.total_pages_for_companies = None
        self.total_pages_for_contacts = None
        self.intercom_config = utils.get_intercom_config()
        self.request_headers = headers
        self.host = self.intercom_config['host']

    def __get_response_for_endpoint(self, endpoint, page):
        request_url = f"{self.host}{endpoint}?per_page={self.intercom_config['per_page']}&page={page}"
        response = rq.get(request_url, headers=self.request_headers)

        return DotMap(json.loads(response.text))

    def get_companies(self, page):
        response_body = self.__get_response_for_endpoint(
            self.intercom_config['companies_endpoint'], page)
        self.total_pages_for_companies = response_body.pages.total_pages

        return response_body.data

    def get_contacts(self, page):
        response_body = self.__get_response_for_endpoint(
            self.intercom_config['contacts_endpoint'], page)
        self.total_pages_for_contacts = response_body.pages.total_pages

        return response_body.data


class IntercomData:
    def __init__(self):
        self.companies = []
        self.contacts = []

    def add_company(self, raw_company, raw_tag):
        parsed_company = {
            'company_id': raw_company.company_id,
            'id': raw_company.id,
            'name': raw_company.name,
            'last_request_at': raw_company.last_request_at,
            'session_count': raw_company.session_count,
            'tag': raw_tag.name,
            'record_ins_timestamp': datetime.now()
        }
        self.companies.append(parsed_company)

    def get_companies(self):
        return self.companies

    def add_contact(self, raw_contact):
        parsed_contact = {
            'contact_id': raw_contact.id,
            'external_id': raw_contact.external_id,
            'name': raw_contact.name,
            'last_seen_at': raw_contact.last_seen_at,
            'type': raw_contact.role,
            'signed_up_at': raw_contact.signed_up_at,
            'city': raw_contact.location.city,
            'record_ins_timestamp': datetime.now()
        }
        self.contacts.append(parsed_contact)

    def get_contacts(self):
        return self.contacts
