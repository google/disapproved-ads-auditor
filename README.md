# Bowling - avoid Google's '3 strikes account suspension'

Starting Sep 21, Google has a new violation policy: when an ad is being disapproved 3 times due to some specific topic violation, Google can suspend a whole account.
The tool audits (and optionally deletes) the disapproved ads that can cause account suspension [Policy Deatils](https://support.google.com/google-ads/answer/10957124?hl=en).


## Disclaimer

**This is not an officially supported Google product.**

Copyright 2021 Google LLC. This solution, including any related sample code or data, is made available on an “as is,” “as available,” and “with all faults” basis, solely for illustrative purposes, and without warranty or representation of any kind. This solution is experimental, unsupported and provided solely for your convenience. Your use of it is subject to your agreements with Google, as applicable, and may constitute a beta feature as defined under those agreements.  To the extent that you make any data available to Google in connection with your use of the solution, you represent and warrant that you have all necessary and appropriate rights, consents and permissions to permit Google to use and process that data.  By using any portion of this solution, you acknowledge, assume and accept all risks, known and unknown, associated with its usage, including with respect to your deployment of any portion of this solution in your systems, or usage in connection with your business, if at all.


Contact: eladb@google.com

## What is it?

A tool to collect all disapproved apps (excluding ads with policy non critical topics from "non_critical_topics.json") in order to avoid account suspension.

Possible actions:
- Only audit the ads
- Audit and remove the ads


## Requirements

- Python 3.9+
- Google Ads API Token (refer to
  [Apply for access to the AdWords API](https://developers.google.com/adwords/api/docs/guides/signup)).
- OAuth 2 Credentials(refer to
  [Generate OAuth2 credentials](https://developers.google.com/adwords/api/docs/guides/authentication#generate_oauth2_credentials)).
- [Enable Google ads API](https://developers.google.com/google-ads/api/docs/first-call/oauth-cloud-project#enable_the_in_your_project)
- Generate Refresh Token (refer to
  [Generate refresh token](https://developers.google.com/google-ads/api/docs/client-libs/python/oauth-desktop#step_3_-_generating_a_refresh_token))

## Setup

1. Clone the repository (reach out to eladb@google.com, dvirka@google.com for access to the user-group: g/solutions_bowling-readers)

```shell
git clone https://professional-services.googlesource.com/solutions/bowling
```

2. Fill in credentials in "google-ads.yaml" file

3. Install google ads API

```shell
pip3 install google-ads==10.0.0
```

4. Install BigQuery API

```shell
pip3 install --user --upgrade google-cloud-bigquery
```

5. Create a GCP service-account (type: desktop-client) and download its key. See [GCP doc] (https://cloud.google.com/docs/authentication/getting-started).
Give that service-account `bigquery.user` role (BigQuery Job User)

6. 
Set an environment variable:

```shell
export GOOGLE_APPLICATION_CREDENTIALS = <YOUR_SERVICE_ACCOUNT_KEY>

```

## Running

1. To run the tool for all accounts under MCC, run the main script with the -id (account_id) flag:

```shell
python3 main.py -id <ACCUNT_ID>
```

### Optional Configurations (flags)

  ```shell
  -p
  ```
- Run the tool in parallel mode for each sub-account:

  ```shell
  -rm
  ```
- Run the tool and immediately delete all ads:

  ```shell
  -bq
  ```
- Write to BigQuery in addition to output file:

  ```shell
  -ddb
  ```
- Delete all relevant tables in BigQuery:

### Python reminder
- Redirect tool's output to file:
  ```shell
  python3 main.py > logFile
  ```

## Output


The results will be saved under the "output" folder (and optionally under BQ dataset "google_3_strikes")


 * "AllAccounts" - lists all the subMCC and sub accounts that were scanned
![_ALL_ACCOUNTS_TABLE_SCHEMA](/images/_ALL_ACCOUNTS_TABLE_SCHEMA.jpg)

account_id
hierarchy: Mcc_SubMcc_SubAccount.
timestamp: when scanning all the sub accounts finished.
session_id: identifies the last run and join with other tables.



 * "AdsToRemove" - list all the ads to be removed *including* all the data required for re-uploading the ads (if they were removed).
![_ADS_TO_REMOVE_TABLE_SCHEMA](/images/_ADS_TO_REMOVE_TABLE_SCHEMA.jpg)

 Based on (Google Ads ad_group_ad report)[https://developers.google.com/google-ads/api/fields/v8/ad_group_ad#ad_group_ad.ad.final_urls]
- ad_id
- ad_type
- ad_group_id
- campaign_id
- hierarchy: Mcc_SubMcc_SubAccount.
- final_urls: The list of possible final URLs after all cross-domain redirects for the ad.
- policy_topics
- evidences
- mandatory_data
- timestamp: when the `bowling_status` was set.
- bowling_status: `SCANNED`, `REMOVED`, `FAILED_TO_REMOVE`.
- account_id
- session_id: identifies the last run and join with other tables.
- removal_error: Google server error in case `bowling_status = FAILED_TO_REMOVE`



 * "PerAccountSummary" - when finished processing an account, it sums the numbers of ads to be removed, ads that have been removed
![_PER_ACCOUNT_SUMMARY_SCHEMA](/images/_PER_ACCOUNT_SUMMARY_SCHEMA.jpg)
- account_id
- ads_to_remove_count: total # of ads to remove.
- timestamp: when scan for this account ended.
- session_id: identifies the last run and join with other tables.


 * "PerMccSummary" - similar sums per top-MCC level
![_PER_MCC_SUMMARY_SCHEMA](/images/_PER_MCC_SUMMARY_SCHEMA.jpg)
- account_id
- total_sub_accounts: total # of sub accounts.
- top_mcc_total_ads_to_remove: total # of ads to remove.
- timestamp: when the scan for the whole mcc ended.
- session_id: identifies the last run and join with other tables.


 ## Example of a relevant SQL query
[SQL query](src/sql/Report.sql)

 ## Notes and recommendations:
 * Run the code as a cron-job over the cloud.
 * Monitor that cron-job with mail alerts when it fails to run.
 * The code does 3 retries if it crashes.
 * Google BQ API allows a built-in retry mechanism (see [BQ query API](https://googleapis.dev/python/bigquery/latest/generated/google.cloud.bigquery.client.Client.html#google.cloud.bigquery.client.Client.query))


 ## Change history
See [CHANGELOG](CHANGELOG.md)
 
 
 ## License
Apache Version 2.0
See [LICENSE](LICENSE)

