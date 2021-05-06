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

class IntercomData:
    def __init__(self):
        self.companies = []
        self.contacts = []
        self.contact_companies = []

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

    def add_contact_company(self, raw_contact, raw_contact_company):
        parsed_contact_company = {
            'contact_id': raw_contact.id,
            'company_id': raw_contact_company.id
        }
        self.contact_companies.append(parsed_contact_company)

    def get_contact_companies(self):
        return self.contact_companies
