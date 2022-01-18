# Copyright 2021 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# !/usr/bin/env python Disclaimer This is not an officially supported Google product. Copyright
# 2021 Google LLC. This solution, including any related sample code or data, is made available on
# an “as is,” “as available,” and “with all faults” basis, solely for illustrative purposes,
# and without warranty or representation of any kind. This solution is experimental, unsupported
# and provided solely for your convenience. Your use of it is subject to your agreements with
# Google, as applicable, and may constitute a beta feature as defined under those agreements. To
# the extent that you make any data available to Google in connection with your use of the
# solution, you represent and warrant that you have all necessary and appropriate rights,
# consents and permissions to permit Google to use and process that data. By using any portion of
# this solution, you acknowledge, assume and accept all risks, known and unknown, associated with
# its usage, including with respect to your deployment of any portion of this solution in your
# systems, or usage in connection with your business, if at all.
#
# Author: Elad Ben David

"""Retrieves and removes disapproved ads for an MCC tree. Collects all disapproved apps (
excluding ads with policy
non critical topics from "non_critical_topics.json") in order to avoid account suspension.

Options to:
- Only audit the ads
- Audit and remove the ads

"""
import argparse
import json
import logging
import re
import sys
import time
import uuid
from concurrent import futures
from pathlib import Path

from google.ads.googleads.errors import GoogleAdsException
from google.cloud import bigquery

from array_utils import split, take_out_elements
from bq_connector import BqServiceWrapper, BowlingStatus
from gads_connector import GAdsServiceWrapper

_DS_ID = "google_3_strikes"
_ALL_ACCOUNTS_TABLE_NAME = "AllAccounts"
_ADS_TO_REMOVE_TABLE_NAME = "AdsToRemove"
_PER_ACCOUNT_SUMMARY_TABLE_NAME = "PerAccountSummary"
_PER_MCC_SUMMARY_TABLE_NAME = "PerMccSummary"
_OUTPUT_PATH = "../output/"
_TOPICS_FILE = './topics_substrings.json'
_CHUNK_SIZE = 5000
_RETRIES_LEFT = 2

logging.basicConfig(level=logging.INFO, format='[%(asctime)s - %(levelname)s] %(message).5000s')
logging.getLogger('google.ads.googleads.client').setLevel(logging.INFO)


def create_bq_tables():
    """Creates BQ required tables"""
    bqServiceWrapper.create_table(_ALL_ACCOUNTS_TABLE_NAME,
                                  [bigquery.SchemaField("account_id", "STRING", mode="REQUIRED"),
                                   bigquery.SchemaField("hierarchy", "STRING", mode="REQUIRED"),
                                   bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"),
                                   bigquery.SchemaField("session_id", "string", mode="REQUIRED")])
    bqServiceWrapper.create_table(_ADS_TO_REMOVE_TABLE_NAME,
                                  [bigquery.SchemaField("ad_id", "STRING", mode="REQUIRED"),
                                   bigquery.SchemaField("ad_type", "STRING", mode="REQUIRED"),
                                   bigquery.SchemaField("ad_group_id", "STRING", mode="REQUIRED"),
                                   bigquery.SchemaField("campaign_id", "STRING", mode="REQUIRED"),
                                   bigquery.SchemaField("hierarchy", "STRING", mode="REQUIRED"),
                                   bigquery.SchemaField("final_urls", "STRING", mode="REQUIRED"),
                                   bigquery.SchemaField("policy_topics", "STRING", mode="REQUIRED"),
                                   bigquery.SchemaField("evidences", "STRING", mode="REQUIRED"),
                                   bigquery.SchemaField("mandatory_data", "STRING",
                                                        mode="REQUIRED"),
                                   bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"),
                                   bigquery.SchemaField("bowling_status", "string",
                                                        mode="NULLABLE"),
                                   bigquery.SchemaField("account_id", "string", mode="NULLABLE"),
                                   bigquery.SchemaField("session_id", "string", mode="REQUIRED"),
                                   bigquery.SchemaField("removal_error", "string",
                                                        mode="NULLABLE")])
    bqServiceWrapper.create_table(_PER_ACCOUNT_SUMMARY_TABLE_NAME,
                                  [bigquery.SchemaField("account_id", "STRING", mode="REQUIRED"),
                                   bigquery.SchemaField("ads_to_remove_count", "INTEGER",
                                                        mode="REQUIRED"),
                                   bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"),
                                   bigquery.SchemaField("session_id", "string", mode="REQUIRED")])

    bqServiceWrapper.create_table(_PER_MCC_SUMMARY_TABLE_NAME,
                                  [bigquery.SchemaField("account_id", "STRING", mode="REQUIRED"),
                                   bigquery.SchemaField("total_sub_accounts", "INTEGER",
                                                        mode="REQUIRED"),
                                   bigquery.SchemaField("top_mcc_total_ads_to_remove", "INTEGER",
                                                        mode="REQUIRED"),
                                   bigquery.SchemaField("accounts_with_ads_to_remove", "INTEGER",
                                                        mode="REQUIRED"),
                                   bigquery.SchemaField("accounts_without_ads_to_remove", "INTEGER",
                                                        mode="REQUIRED"),
                                   bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"),
                                   bigquery.SchemaField("session_id", "string", mode="REQUIRED")])


def main(top_id):
    """Gets all the accounts, logs the crucial disapproved ads and optionally removes them"""
    top_id = top_id.replace("-", "")
    accounts_with_removed_ads = 0
    accounts_without_removed_ads = 0
    top_mcc_total_removed_ads = 0
    if _WRITE_TO_BQ:
        create_bq_tables()
    accounts = flat_all_accounts(top_id, str(top_id))
    write_to_file(_ALL_ACCOUNTS_TABLE_NAME, accounts)
    if _WRITE_TO_BQ:
        bqServiceWrapper.upload_rows_to_bq(table_id=_ALL_ACCOUNTS_TABLE_NAME,
                                           rows_to_insert=accounts)
    if _PARALLEL_MODE:
        with futures.ThreadPoolExecutor() as executor:
            results = executor.map(
                lambda account_item: remove_disapproved_ads_for_account(account_item), accounts)
        for removed_ads_count in results:
            top_mcc_total_removed_ads += removed_ads_count
            if removed_ads_count > 0:
                accounts_with_removed_ads += 1
            else:
                accounts_without_removed_ads += 1
    else:
        for account in accounts:
            removed_ads_count = remove_disapproved_ads_for_account(account)
            if removed_ads_count > 0:
                accounts_with_removed_ads += 1
            else:
                accounts_without_removed_ads += 1
    per_mcc_summary = {
        f"\ntop_mcc_total_accounts = {accounts_with_removed_ads + accounts_without_removed_ads}, "
        f"accounts_with_removed_ads = {accounts_with_removed_ads}, "
        f"accounts_without_removed_ads = {accounts_without_removed_ads}, "
        f"top_mcc_total_removed_ads = {top_mcc_total_removed_ads}"}
    print(per_mcc_summary)
    write_to_file(_PER_MCC_SUMMARY_TABLE_NAME, per_mcc_summary)
    if _WRITE_TO_BQ:
        bqServiceWrapper.upload_rows_to_bq(table_id=_PER_MCC_SUMMARY_TABLE_NAME, rows_to_insert=[
            add_session_identifiers_bq_columns(
                {"account_id": top_id, "accounts_with_ads_to_remove": accounts_with_removed_ads,
                 "accounts_without_ads_to_remove": accounts_without_removed_ads,
                 "top_mcc_total_ads_to_remove": top_mcc_total_removed_ads,
                 "total_sub_accounts": accounts_with_removed_ads + accounts_without_removed_ads})])


def flat_all_accounts(account_id, hierarchy):
    """Returns a list {id, hierarchy} for all the descendant accounts of a given MCC account"""
    accounts = gAdsServiceWrapper.get_sub_accounts(False, account_id, hierarchy)
    accounts.append({"account_id": account_id, "hierarchy": hierarchy})
    sub_mccs = gAdsServiceWrapper.get_sub_accounts(True, account_id, hierarchy)
    if len(sub_mccs) > 0:
        for sub_mcc in sub_mccs:
            accounts = accounts + flat_all_accounts(sub_mcc["account_id"], sub_mcc["hierarchy"])
    return [add_session_identifiers_bq_columns(account) for account in accounts]


def remove_disapproved_ads_for_account(account):
    """Remove all disapproved ads for a given customer id"""
    account_id = account["account_id"]
    ad_removal_operations = []
    ads_to_remove_json = []
    rows = gAdsServiceWrapper.get_disapproved_ads_for_account(account_id)
    ads_to_remove_count = 0
    print(f"\nProcessing Account id: {account_id} =============")

    for batch in rows:
        for row in batch.results:
            ad_group_ad = row.ad_group_ad
            campaign_id = row.campaign.id
            ad = ad_group_ad.ad
            policy_summary = ad_group_ad.policy_summary
            current_topics = [entry.topic.lower() for entry in policy_summary.policy_topic_entries]
            if has_included_topic(current_topics, _INCLUDED_TOPICS_SUBSTRINGS,
                                  _EXCLUDED_TOPICS_SUBSTRINGS):
                ads_to_remove_count += 1
                print('** A suspension topic, will be removed')
                print(f'\ttopics: "{current_topics}"')
                ad_json = get_ad_hierarchy(account, campaign_id, ad_group_ad, ad)
                ad_json["policy_topics"] = str(current_topics)
                ad_json["evidences"] = str(get_policy_extra(policy_summary))
                populate_ad_json_mandatory_data(ad_json, ad_group_ad, ad)
                ads_to_remove_json.append(ad_json)
                if _REMOVE_ADS:
                    ad_removal_operations.append(
                        build_ad_removal_sync_operation(account_id, ad_json["ad_group_id"],
                                                        row.ad_group_ad.ad.id))

    if len(ads_to_remove_json) > 0:
        ads_to_remove_json = audit_ads_before_remove(ads_to_remove_json)
    if len(ad_removal_operations) > 0:
        remove_ads(ad_removal_operations, ads_to_remove_json, account_id)
    audit_ads_after_remove(account_id, ads_to_remove_count)
    return ads_to_remove_count


def add_session_identifiers_bq_columns(item):
    """Adds session identifiers BQ columns"""
    item["timestamp"] = "AUTO"
    item["session_id"] = CURRENT_SESSION_ID
    return item


def add_bq_columns_to_ad(ad_removal_item, status):
    """Populates status and identifiers BQ columns"""
    ad_removal_item["bowling_status"] = status
    add_session_identifiers_bq_columns(ad_removal_item)
    return ad_removal_item


def audit_ads_after_remove(account_id, ads_to_remove_count):
    """Audits ads after removal"""
    data = {f"\nAccount-id: {account_id} ============= Finished Processing. # relevant disapproved "
            f"ads found: {str(ads_to_remove_count)}"}
    write_to_file(_PER_ACCOUNT_SUMMARY_TABLE_NAME, data)
    if _WRITE_TO_BQ:
        bqServiceWrapper.upload_rows_to_bq(table_id=_PER_ACCOUNT_SUMMARY_TABLE_NAME,
                                           rows_to_insert=[add_session_identifiers_bq_columns(
                                               {"account_id": account_id,
                                                "ads_to_remove_count": ads_to_remove_count})])


def audit_ads_before_remove(ads_to_be_removed_json):
    """Audits ads before removal"""
    ads_to_be_removed_json = [add_bq_columns_to_ad(ad_removal_item, BowlingStatus.SCANNED.name) for
                              ad_removal_item in ads_to_be_removed_json]
    write_to_file(_ADS_TO_REMOVE_TABLE_NAME, ads_to_be_removed_json)
    if _WRITE_TO_BQ:
        bqServiceWrapper.upload_rows_to_bq(table_id=_ADS_TO_REMOVE_TABLE_NAME,
                                           rows_to_insert=ads_to_be_removed_json)
    return ads_to_be_removed_json


def get_full_output_path(file_name):
    """Return full output path"""
    return Path(f"{_OUTPUT_PATH}/{file_name}_{time.strftime('%Y%m%d-%H%M%S')}.json")


def write_to_file(file, content):
    """Writes to file"""
    with open(get_full_output_path(file), 'a') as file_object:
        file_object.write(
            "\n" + json.dumps(content, default=lambda x: list(x) if isinstance(x, set) else x))


def get_policy_extra(policy_summary):
    """Returns 'Policy Extra' data"""
    evidence_array = []
    # Display the policy topic entries related to the ad disapproval.
    for entry in policy_summary.policy_topic_entries:
        print(f'\ttopic: "{entry.topic}", type "{entry.type_.name}"')
        # Display the attributes and values that triggered the policy
        # topic.
        evidence_array_per_entry = []
        for evidence in entry.evidences:
            for index, text in enumerate(evidence.text_list.texts):
                evidence_array_per_entry.append(f"\t\tevidence text[{index}]: {text}")
        evidence_array.append(
            {"topic": entry.topic, "type": entry.type_.name, "array": evidence_array_per_entry})
    return evidence_array


def populate_errors(failed_items, errors):
    """Populate ads with corresponding removal errors"""
    for item, error in zip(failed_items, errors):
        item["bowling_status"] = {BowlingStatus.FAILED_TO_REMOVE.name}
        item["removal_error"] = error


def update_status_removed(removed_items):
    """Update ads with status removed"""
    for item in removed_items:
        item["bowling_status"] = {BowlingStatus.REMOVED.name}


def remove_ads(removal_operations, removal_json, account_id):
    """Removes ads"""
    operations_chucks = split(removal_operations, _CHUNK_SIZE)
    json_request_chunks = split(removal_json, _CHUNK_SIZE)
    for chunk_index, operations_chuck in enumerate(operations_chucks):
        try:
            response_chunk = send_bulk_mutate_request(account_id, operations_chuck)
        except GoogleAdsException as exception:
            handle_googleads_exception(exception)
        else:
            # Remove succeeded
            index_array, error_array = _print_results(response_chunk)
            removed_items = json_request_chunks[chunk_index]
            failed_items = take_out_elements(removed_items, index_array)
            update_status_removed(failed_items)
            populate_errors(failed_items, error_array)
            all_items = removed_items + failed_items
            write_to_file(_ADS_TO_REMOVE_TABLE_NAME, all_items)
            if _WRITE_TO_BQ:
                bqServiceWrapper.upload_rows_to_bq(table_id=_ADS_TO_REMOVE_TABLE_NAME,
                                                   rows_to_insert=all_items)


def get_ad_hierarchy(account, campaign_id, ad_group_ad, ad):
    """Returns ad hierarchy"""
    match_groups = re.match(r"customers/(\w+)/adGroups/(\w+)", ad_group_ad.ad_group)
    if match_groups is not None:
        ad_group_id = match_groups.group(2)
    else:
        ad_group_id = ad_group_ad.ad_group
    return {"hierarchy": account["hierarchy"], "account_id": account["account_id"],
            "campaign_id": campaign_id, "ad_group_id": ad_group_id, "ad_id": ad.id,
            "ad_type": ad.type_.name, "final_urls": ', '.join(ad_group_ad.ad.final_urls)}


def populate_ad_json_mandatory_data(ad_json, ad_group_ad, ad):
    """Populates ad json with 'mandatory_data' """
    if ad.type_.name.upper() == "TEXT_AD":
        mandatory_data = {"ad.text_ad.headline": ad.text_ad.headline,
                          "desc1": ad.text_ad.description1, "desc2": ad.text_ad.description2}
    elif ad.type_.name.upper() == "EXPANDED_TEXT_AD":
        mandatory_data = {
            'ad_group_ad.ad.expanded_text_ad.description':
                ad_group_ad.ad.expanded_text_ad.description,
            'ad_group_ad.ad.expanded_text_ad.description2':
                ad_group_ad.ad.expanded_text_ad.description2,
            'ad_group_ad.ad.expanded_text_ad.headline_part1':
                ad_group_ad.ad.expanded_text_ad.headline_part1,
            'ad_group_ad.ad.expanded_text_ad.headline_part2':
                ad_group_ad.ad.expanded_text_ad.headline_part2,
            'ad_group_ad.ad.expanded_text_ad.headline_part3':
                ad_group_ad.ad.expanded_text_ad.headline_part3}
    elif ad.type_.name.upper() == "RESPONSIVE_SEARCH_AD":
        mandatory_data = {"ad_group_ad.ad.responsive_search_ad.headlines": extract_text_from_proto(
            ad_group_ad.ad.responsive_search_ad.headlines),
            "ad_group_ad.ad.responsive_search_ad.descriptions": extract_text_from_proto(
                ad_group_ad.ad.responsive_search_ad.descriptions),
            "ad_group_ad.ad.responsive_search_ad.path1": ad_group_ad.ad.responsive_search_ad.path1,
            "ad_group_ad.ad.responsive_search_ad.path2": ad_group_ad.ad.responsive_search_ad.path2}
    else:
        mandatory_data = {"type": ad.type_.name.upper()}
    ad_json["mandatory_data"] = str(mandatory_data)


def extract_text_from_proto(proto_list):
    """Extracts text from proto"""
    values = []
    for item in proto_list:
        if item.pinned_field:
            values.append("%s: %s" % (item.pinned_field, item.text))
        else:
            values.append("%s" % item.text)
    return values


def load_included_topics():
    """Loads topics non crucial list (exclusion list)"""
    with open(_TOPICS_FILE, encoding='utf-8-sig') as file_object:
        substring_inclusion_list = json.load(file_object)["only_these_substrings"]
        print(substring_inclusion_list)
        return substring_inclusion_list


def load_excluded_topics():
    """Loads topics non crucial list (exclusion list)"""
    with open(_TOPICS_FILE, encoding='utf-8-sig') as file_object:
        substring_exclusion_list = json.load(file_object)["anything_but_these_substrings"]
        print(substring_exclusion_list)
        return substring_exclusion_list


def has_included_topic(current_topics, inclusion_topics_substrings, exclusion_topics_substrings):
    """Checks if the topic list contains a critical topic"""
    for current_topic in current_topics:
        if is_included_topic(current_topic, inclusion_topics_substrings, True) or is_included_topic(
            current_topic, exclusion_topics_substrings, False):
            return True
    return False


def is_included_topic(current_topic, topics_substrings, isInclusionList):
    """Checks if a given topic is critical"""
    for topic_substring in topics_substrings:
        if topic_substring in current_topic:
            return isInclusionList
    return not isInclusionList


def build_ad_removal_sync_operation(account_id, ad_group_id, ad_id):
    """Builds ad removal sync operation"""
    resource_name = gAdsServiceWrapper.ad_group_ad_service.ad_group_ad_path(account_id, ad_group_id,
                                                                            ad_id)
    ad_group_ad_op1 = gAdsServiceWrapper.client.get_type("AdGroupAdOperation")
    ad_group_ad_op1.remove = resource_name
    return ad_group_ad_op1


def send_bulk_mutate_request(account_id, operations):
    """Sends a bulk mutate request"""
    # Issue a mutate request, setting partial_failure=True.
    request = gAdsServiceWrapper.client.get_type("MutateAdGroupAdsRequest")
    request.customer_id = account_id
    request.operations = operations
    request.partial_failure = True
    return gAdsServiceWrapper.ad_group_ad_service.mutate_ad_group_ads(request=request)


# [START handle_partial_failure_1]
def _is_partial_failure_error_present(response):
    """Checks whether a response message has a partial failure error.
    Args:
        response:  A MutateAdGroupsResponse message instance.
    Returns: A boolean, whether or not the response message has a partial
        failure error.
    """
    partial_failure = getattr(response, "partial_failure_error", None)
    code = getattr(partial_failure, "code", None)
    return code != 0  # [END handle_partial_failure_1]


# [START handle_partial_failure_2]
def _print_results(response):
    """Prints partial failure errors and success messages from a response.
    Args:
        response: a MutateAdGroupsResponse instance.
    """
    index_array = []
    error_array = []

    # Check for existence of any partial failures in the response.
    if _is_partial_failure_error_present(response):
        print("Partial failures occurred. Details will be shown below.\n")
        # Prints the details of the partial failure errors.
        partial_failure = getattr(response, "partial_failure_error", None)
        # partial_failure_error.details is a repeated field and iterable
        error_details = getattr(partial_failure, "details", [])

        for error_detail in error_details:
            # Retrieve an instance of the google_ads_failure class from the client
            failure_message = gAdsServiceWrapper.client.get_type("GoogleAdsFailure")
            # Parse the string into a GoogleAdsFailure message instance.
            # To access class-only methods on the message we retrieve its type.
            google_ads_failure = type(failure_message)
            failure_object = google_ads_failure.deserialize(error_detail.value)

            for error in failure_object.errors:
                # Construct and print a string that details which element in
                # the above ad_group_operations list failed (by index number)
                # as well as the error message and error code.
                print("A partial failure at index "
                      f"{error.location.field_path_elements[0].index} occurred "
                      f"\nError message: {error.message}\nError code: "
                      f"{error.error_code}")
                index_array.append(error.location.field_path_elements[0].index)
                error_array.append(
                    {"error_message": str(error.message), "error_code": str(error.error_code)})
    else:
        print("All operations completed successfully. No partial failure "
              "to show.")

    # In the list of results, operations from the ad_group_operation list
    # that failed will be represented as empty messages. This loop detects
    # such empty messages and ignores them, while printing information about
    # successful operations.
    for message in response.results:
        if not message:
            continue
        print(f"Removed ad group ad with resource_name: {message.resource_name}.")
    return index_array, error_array


def handle_googleads_exception(exception):
    """Prints the details of a GoogleAdsException object.
    Args:
        exception: an instance of GoogleAdsException.
    """
    print(f'Request with ID "{exception.request_id}" failed with status '
          f'"{exception.error.code().name}" and includes the following errors:')
    for error in exception.failure.errors:
        print(f'\tError with message "{error.message}".')
        if error.location:
            for field_path_element in error.location.field_path_elements:
                print(f"\t\tOn field: {field_path_element.field_name}")
    sys.exit(1)


def delete_tables():
    """Deletes BQ tables"""
    bqServiceWrapper.delete_table(_PER_MCC_SUMMARY_TABLE_NAME)
    bqServiceWrapper.delete_table(_ADS_TO_REMOVE_TABLE_NAME)
    bqServiceWrapper.delete_table(_ALL_ACCOUNTS_TABLE_NAME)
    bqServiceWrapper.delete_table(_PER_ACCOUNT_SUMMARY_TABLE_NAME)


def create_results_folder(output_path):
    """Creates results folder"""
    Path(output_path).mkdir(parents=True, exist_ok=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Lists disapproved ads for a given top MCC")
    parser.add_argument("-id", "--top_id", type=str, required=True,
                        help="The Google Ads top mcc ID.", )
    parser.add_argument("-seq", "--sequential", action="store_true",
                        help="Runs multiple accounts in parallel.", )
    parser.add_argument("-rm", "--remove_ads", action="store_true",
                        help="Should remove disapproved ads.", )
    parser.add_argument("-bq", "--write_to_bq", action="store_true",
                        help="Write output to BQ in addition to a local file.", )
    parser.add_argument("-ddb", "--delete_db", action="store_true", help="Delete DB tables.", )
    parser.add_argument("-clean_bq", "--clean_outdated_bq", action="store_true",
                        help="Clean outdated rows in BQ.", )

    args = parser.parse_args()
    _REMOVE_ADS = args.remove_ads
    _PARALLEL_MODE = not args.sequential
    _WRITE_TO_BQ = args.write_to_bq

    _INCLUDED_TOPICS_SUBSTRINGS = load_included_topics()
    _EXCLUDED_TOPICS_SUBSTRINGS = [] if len(
        _INCLUDED_TOPICS_SUBSTRINGS) == 0 else load_excluded_topics()
    CURRENT_SESSION_ID = str(uuid.uuid4())
    while _RETRIES_LEFT > 0:
        _RETRIES_LEFT -= 1
        try:
            create_results_folder(_OUTPUT_PATH)
            if _WRITE_TO_BQ:
                bqServiceWrapper = BqServiceWrapper(_DS_ID)
                if args.delete_db:
                    delete_tables()
                    time.sleep(30)  # Number of seconds
                elif args.clean_outdated_bq:
                    bqServiceWrapper.remove_outdated_scanned_rows(_ADS_TO_REMOVE_TABLE_NAME)
            gAdsServiceWrapper = GAdsServiceWrapper(args.top_id)
            main(args.top_id)
            sys.exit(0)
        except GoogleAdsException as ex:
            handle_googleads_exception(ex)
