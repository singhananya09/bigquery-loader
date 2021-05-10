import json

import requests as rq
from dotmap import DotMap

from helperutils import HelperUtils


class JiraLoader:
    def __init__(self):
        utils = HelperUtils()
        self.jira_config = utils.get_jira_config()
        self.auth_username = utils.get_jira_auth_username()
        self.auth_token = utils.get_jira_auth_token()
        self.request_url = f"{self.jira_config['host']}?&jql={self.jira_config['jql_filter']}"

    def get_issues(self, start_at):
        request_url = f"{self.request_url}&startAt={start_at}"
        print(f"GET {request_url}")
        response = rq.get(request_url, auth=(
            self.auth_username, self.auth_token))
        response_body = DotMap(json.loads(response.text))

        return response_body.issues
