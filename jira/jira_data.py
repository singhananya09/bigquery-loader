from datetime import datetime


class JiraData:
    def __init__(self):
        self.issues = []

    def add_issue(self, raw_issue, raw_sprint):
        parsed_issue = {
            'issue_id': raw_issue.id,
            'issue_key': raw_issue.key,
            'issue_status': raw_issue.fields.status.name,
            'issue_created_date': raw_issue.fields.created,
            'project_id': raw_issue.fields.project.id,
            'project_name': raw_issue.fields.project.name,
            'story_point': raw_issue.fields.customfield_10028,
            'sprint_id': raw_sprint.id,
            'sprint_board_id': raw_sprint.boardId,
            'sprint_name': raw_sprint.name,
            'sprint_status': raw_sprint.state,
            'sprint_start_date': raw_sprint.startDate,
            'sprint_end_date': raw_sprint.endDate,
            'record_ins_timestamp': datetime.now()
        }
        self.issues.append(parsed_issue)

    def get_issues(self):
        return self.issues
