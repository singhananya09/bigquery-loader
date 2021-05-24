# Be sure to run `pip install -r requirements.txt`
# before running this script!

from hubspot_dataframe import hubspot_init
from intercom_dataframe import intercom_init
from jira_dataframe import jira_init
import os

# Change working directory
os.chdir(os.path.dirname(__file__))


def main():
    jira_init()
    intercom_init()
    hubspot_init()


# Call Main Function
if __name__ == "__main__":
    main()
