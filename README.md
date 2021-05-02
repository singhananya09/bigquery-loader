## BigQuery Loader

This project fetches data from OnPlan Jira and Intercom instances, parses and
transforms data into a dataframe object and then loads it into the data warehouse
within Google BigQuery.

#### Prerequisites

To run the script, you would need [Python `v3.7.9`](https://www.python.org/downloads/release) or higher installed.

There are certain steps that you need to follow before you can run
the script, those are as follows;

1. The script makes REST calls to OnPlan Jira and Intercom instances,
   which requires authentication token to be passed in request header.
   You need to configure the environment variables beforehand. These
   variables can either be configured from your Cloud provider's console
   or by following operating system instructions. Following are the
   environment variables that script uses;
   - `JIRA_USERNAME`: User ID (i.e. email address) of Jira account.
   - `JIRA_TOKEN`: Authentication token associated with Jira account.
   - `INTERCOM_TOKEN`: Authentication token associated with Intercom account.
2. Once data is fetched from Jira and Intercom via its respective REST API,
   it is then loaded into Google BigQuery as dataframe object. The BigQuery
   API requires a private key file (`.json`) to be provided before data can be loaded.
   You need to obtain this file from your organization's Google Cloud Platform
   administrator. Once the file is available, place it in the root folder of this
   project and rename it to `dw_credentials.json` (it can be named as anything but
   you would have to update the `config.yml`'s `big_query.credentials` property
   value with the file name).
3. After environment variables are set and private key file is available, install
   project dependencies by running `pip install -r requirements.txt`. Please note
   that if you're running Python 3 along side Python 2, `pip` command may still
   be pointing to Python too, in which case, you would run `pip3 install -r requirements.txt`.

##### `config.yml`

The `config.yml` file keeps configuration that script primarily relies on, refer to
the inline comments for each property within this file to understand what it does.

#### How to run?

Once all the steps as mentioned in _prerequisites_ section are done, run the script
by running `python main.py`.

### Author

[Ananya Singh](https://www.linkedin.com/in/ananyaasingh/)