from datetime import datetime


class HubspotData:
    def __init__(self):
        self.company_deals = []

    def add_company_deal(self, raw_deal):
        parsed_company_deal = {
            'company_id': raw_deal.associations.associatedCompanyIds[0],
            'deal_id': raw_deal.dealId,
            'record_ins_timestamp': datetime.now()
        }
        self.company_deals.append(parsed_company_deal)

    def get_company_deals(self):
        return self.company_deals
