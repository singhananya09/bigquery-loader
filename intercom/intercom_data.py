from datetime import datetime


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
