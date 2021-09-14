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
"""Retrieves and removes disapproved ads for an MCC tree."""
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
from bq_connector import BqServiceWrapper
from gads_connector import GAdsServiceWrapper

_DS_ID = "google_3_strikes"
_ALL_ACCOUNTS_TABLE_NAME = "AllAccounts"
_ADS_TO_REMOVE_TABLE_NAME = "AdsToRemove"
_PER_ACCOUNT_SUMMARY_TABLE_NAME = "PerAccountSummary"
_PER_MCC_SUMMARY_TABLE_NAME = "PerMccSummary"

_REMOVE_ADS = None
_PARALLEL_MODE = None
_CHUNK_SIZE = 5000
_TRIES_LEFT = 3

logging.basicConfig(level=logging.INFO, format='[%(asctime)s - %(levelname)s] %(message).5000s')
logging.getLogger('google.ads.googleads.client').setLevel(logging.INFO)

_NON_CRITICAL_TOPICS = [each_string.lower() for each_string in ['Destination', 'format']]
_DEBUG_SUSPENSION_TOPICS = [each_string.lower() for each_string in ['Destination_not_working']]

_NON_CRITICAL_TOPICS_FILE = './non_critical_topics.json'
_DISAPPROVED_ADS_AUDIT_FILE_NAME = 'disapproved_ads_' + time.strftime("%Y%m%d-%H%M%S")
_DISAPPROVED_ADS_AUDIT_FILE_PATH = Path('./results/' + _DISAPPROVED_ADS_AUDIT_FILE_NAME + '.json')


def create_bq_tables():
    bqServiceWrapper.create_table(_ALL_ACCOUNTS_TABLE_NAME,
                                  [
                                      bigquery.SchemaField("account_id", "STRING", mode="REQUIRED"),
                                      bigquery.SchemaField("hierarchy", "STRING", mode="REQUIRED"),
                                      bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"),
                                      bigquery.SchemaField("session_id", "string", mode="REQUIRED")
                                  ])
    bqServiceWrapper.create_table(_ADS_TO_REMOVE_TABLE_NAME,
                                  [
                                      bigquery.SchemaField("ad_id", "STRING", mode="REQUIRED"),
                                      bigquery.SchemaField("ad_type", "STRING", mode="REQUIRED"),
                                      bigquery.SchemaField("ad_group_id", "STRING", mode="REQUIRED"),
                                      bigquery.SchemaField("campaign_id", "STRING", mode="REQUIRED"),
                                      bigquery.SchemaField("hierarchy", "STRING", mode="REQUIRED"),
                                      bigquery.SchemaField("final_urls", "STRING", mode="REQUIRED"),
                                      bigquery.SchemaField("policy_topics", "STRING", mode="REQUIRED"),
                                      bigquery.SchemaField("evidences", "STRING", mode="REQUIRED"),
                                      bigquery.SchemaField("mandatory_data", "STRING", mode="REQUIRED"),
                                      bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"),
                                      bigquery.SchemaField("status", "string", mode="NULLABLE"),
                                      bigquery.SchemaField("customer_id", "string", mode="NULLABLE"),
                                      bigquery.SchemaField("session_id", "string", mode="REQUIRED"),
                                      bigquery.SchemaField("removal_error", "string", mode="NULLABLE")
                                  ])
    bqServiceWrapper.create_table(_PER_ACCOUNT_SUMMARY_TABLE_NAME,
                                  [
                                      bigquery.SchemaField("account_id", "STRING", mode="REQUIRED"),
                                      bigquery.SchemaField("ads_to_remove_count", "INTEGER", mode="REQUIRED"),
                                      bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"),
                                      bigquery.SchemaField("session_id", "string", mode="REQUIRED")
                                  ])

    bqServiceWrapper.create_table(_PER_MCC_SUMMARY_TABLE_NAME,
                                  [
                                      bigquery.SchemaField("account_id", "STRING", mode="REQUIRED"),
                                      bigquery.SchemaField("total_sub_accounts", "INTEGER", mode="REQUIRED"),
                                      bigquery.SchemaField("top_mcc_total_ads_to_remove", "INTEGER", mode="REQUIRED"),
                                      bigquery.SchemaField("accounts_with_ads_to_remove", "INTEGER", mode="REQUIRED"),
                                      bigquery.SchemaField("accounts_without_ads_to_remove", "INTEGER",
                                                           mode="REQUIRED"),
                                      bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"),
                                      bigquery.SchemaField("session_id", "string", mode="REQUIRED")
                                  ])


def main(top_id):
    accounts_with_removed_ads = 0
    accounts_without_removed_ads = 0
    top_mcc_total_removed_ads = 0
    create_bq_tables()
    accounts = flat_all_accounts(top_id, str(top_id))
    bqServiceWrapper.upload_rows_to_bq(table_id=_ALL_ACCOUNTS_TABLE_NAME, rows_to_insert=accounts)
    if _PARALLEL_MODE:
        with futures.ThreadPoolExecutor() as executor:
            results = executor.map(
                lambda account_item: remove_disapproved_ads_for_account(account_item),
                accounts)
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
    print(
        f"\ntop_mcc_total_accounts = {accounts_with_removed_ads + accounts_without_removed_ads}, "
        f"accounts_with_removed_ads = {accounts_with_removed_ads}, "
        f"accounts_without_removed_ads = {accounts_without_removed_ads}, "
        f"top_mcc_total_removed_ads = {top_mcc_total_removed_ads}")
    bqServiceWrapper.upload_rows_to_bq(
        table_id=_PER_MCC_SUMMARY_TABLE_NAME,
        rows_to_insert=[add_session_identifiers_bq_columns({"account_id": top_id,
                                                            "accounts_with_ads_to_remove": accounts_with_removed_ads,
                                                            "accounts_without_ads_to_remove":
                                                                accounts_without_removed_ads,
                                                            "top_mcc_total_ads_to_remove": top_mcc_total_removed_ads,
                                                            "total_sub_accounts": accounts_with_removed_ads + accounts_without_removed_ads})])


def flat_all_accounts(customer_id, hierarchy):
    """Returns a list {id, hierarchy} for all the descendant accounts of a given MCC account"""
    accounts = gAdsServiceWrapper.get_sub_accounts(False, customer_id, hierarchy)
    accounts.append({"account_id": customer_id, "hierarchy": hierarchy})
    sub_mccs = gAdsServiceWrapper.get_sub_accounts(True, customer_id, hierarchy)
    if len(sub_mccs) > 0:
        for sub_mcc in sub_mccs:
            accounts = accounts + flat_all_accounts(sub_mcc["account_id"], sub_mcc["hierarchy"])
    return [add_session_identifiers_bq_columns(account) for account in accounts]


def remove_disapproved_ads_for_account(account):
    """Remove all disapproved ads for a given customer id"""
    customer_id = account["account_id"]
    ad_removal_operations = []
    ads_to_remove_json = []
    rows = gAdsServiceWrapper.get_disapproved_ads_for_account(customer_id)
    ads_to_remove_count = 0
    print(f"\nProcessing Account id: {customer_id} =============")

    for batch in rows:
        for row in batch.results:
            ad_group_ad = row.ad_group_ad
            campaign_id = row.campaign.id
            ad = ad_group_ad.ad
            policy_summary = ad_group_ad.policy_summary
            current_topics = [entry.topic.lower() for entry in policy_summary.policy_topic_entries]
            if does_contain_critical_topics(current_topics, _NON_CRITICAL_TOPICS):
                ads_to_remove_count += 1
                print(f'** A suspension topic, will be removed')
                print(f'\ttopics: "{current_topics}"')
                ad_json = get_ad_hierarchy(account, campaign_id, ad_group_ad, ad)
                ad_json["policy_topics"] = str(current_topics)
                ad_json["evidences"] = str(get_policy_extra(policy_summary))
                populate_ad_json_full_details(ad_json, ad_group_ad, ad)
                ads_to_remove_json.append(ad_json)
                if _REMOVE_ADS:
                    ad_removal_operations.append(
                        build_ad_removal_sync_operation(customer_id, ad_json["ad_group_id"], row.ad_group_ad.ad.id))

    if len(ads_to_remove_json) > 0:
        ads_to_remove_json = audit_ads_before_remove(ads_to_remove_json)
    if len(ad_removal_operations) > 0:
        remove_ads(ad_removal_operations, ads_to_remove_json, customer_id)
    audit_ads_after_remove(customer_id, ads_to_remove_count)
    return ads_to_remove_count


def add_session_identifiers_bq_columns(item):
    item["timestamp"] = "AUTO"
    item["session_id"] = current_session_id
    return item


def add_bq_columns_to_ad(ad_removal_item, status):
    ad_removal_item["status"] = status
    add_session_identifiers_bq_columns(ad_removal_item)
    return ad_removal_item


def audit_ads_after_remove(account_id, ads_to_remove_count):
    print(
        f"\nAccount-id: {account_id} ============= Finished Processing. # relevant disapproved ads found: "
        f"{str(ads_to_remove_count)}")
    bqServiceWrapper.upload_rows_to_bq(
        table_id=_PER_ACCOUNT_SUMMARY_TABLE_NAME,
        rows_to_insert=[add_session_identifiers_bq_columns({"account_id": account_id,
                                                            "ads_to_remove_count": ads_to_remove_count})])


def audit_ads_before_remove(ads_to_be_removed_json):
    ads_to_be_removed_json = [add_bq_columns_to_ad(ad_removal_item, None)
                              for ad_removal_item in ads_to_be_removed_json]
    with open(Path(_DISAPPROVED_ADS_AUDIT_FILE_PATH), 'a') as f:
        f.write("\n" + json.dumps(ads_to_be_removed_json))
    bqServiceWrapper.upload_rows_to_bq(table_id=_ADS_TO_REMOVE_TABLE_NAME, rows_to_insert=ads_to_be_removed_json)
    return ads_to_be_removed_json


def get_policy_extra(policy_summary):
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
    for item, error in zip(failed_items, errors):
        item["removal_error"] = error


def remove_ads(removal_operations, removal_json, customer_id):
    operations_chucks = split(removal_operations, _CHUNK_SIZE)
    json_chunks = split(removal_json, _CHUNK_SIZE)
    for chunk_index, operations_chuck in enumerate(operations_chucks):
        try:
            chunk_reponse = send_bulk_mutate_request(customer_id, operations_chuck)
        except GoogleAdsException as ex:
            handle_googleads_exception(ex)
        else:
            # Remove succeeded
            index_array, error_array = _print_results(chunk_reponse)
            removed_items = json_chunks[chunk_index]
            failed_items = take_out_elements(removed_items, index_array)

            bqServiceWrapper.update_bq_ads_status_removed(table_id=_ADS_TO_REMOVE_TABLE_NAME,
                                                          update_ads=removed_items)

            populate_errors(failed_items, error_array)
            bqServiceWrapper.update_bq_ads_status_failed(table_id=_ADS_TO_REMOVE_TABLE_NAME,
                                                         update_ads=failed_items)


def get_ad_hierarchy(account, campaign_id, ad_group_ad, ad):
    m = re.match(r"customers/(\w+)/adGroups/(\w+)", ad_group_ad.ad_group)
    if m is not None:
        ad_group_id = m.group(2)
    else:
        ad_group_id = ad_group_ad.ad_group
    return {"hierarchy": account["hierarchy"],
            "customer_id": account["account_id"],
            "campaign_id": campaign_id,
            "ad_group_id": ad_group_id,
            "ad_id": ad.id,
            "ad_type": ad.type_.name,
            "final_urls": ', '.join(ad_group_ad.ad.final_urls)}


def populate_ad_json_full_details(ad_json, ad_group_ad, ad):
    mandatory_data = {}
    if ad.type_.name.upper() == "TEXT_AD":
        mandatory_data = {
            "ad.text_ad.headline": ad.text_ad.headline,
            "desc1": ad.text_ad.description1,
            "desc2": ad.text_ad.description2}
    elif ad.type_.name.upper() == "EXPANDED_TEXT_AD":
        mandatory_data = {
            'ad_group_ad.ad.expanded_text_ad.description': ad_group_ad.ad.expanded_text_ad.description,
            'ad_group_ad.ad.expanded_text_ad.description2': ad_group_ad.ad.expanded_text_ad.description2,
            'ad_group_ad.ad.expanded_text_ad.headline_part1': ad_group_ad.ad.expanded_text_ad.headline_part1,
            'ad_group_ad.ad.expanded_text_ad.headline_part2': ad_group_ad.ad.expanded_text_ad.headline_part2,
            'ad_group_ad.ad.expanded_text_ad.headline_part3': ad_group_ad.ad.expanded_text_ad.headline_part3}
    elif ad.type_.name.upper() == "RESPONSIVE_SEARCH_AD":
        mandatory_data = {
            "ad_group_ad.ad.responsive_search_ad.headlines": extract_text_from_proto(
                ad_group_ad.ad.responsive_search_ad.headlines),
            "ad_group_ad.ad.responsive_search_ad.descriptions": extract_text_from_proto(
                ad_group_ad.ad.responsive_search_ad.descriptions),
            "ad_group_ad.ad.responsive_search_ad.path1": ad_group_ad.ad.responsive_search_ad.path1,
            "ad_group_ad.ad.responsive_search_ad.path2": ad_group_ad.ad.responsive_search_ad.path2}
    else:
        print(ad.type_.name.upper())
    ad_json["mandatory_data"] = str(mandatory_data)


def extract_text_from_proto(proto_list):
    values = []
    for item in proto_list:
        if item.pinned_field:
            values.append("%s: %s" % (item.pinned_field, item.text))
        else:
            values.append("%s" % item.text)
    return values


def extract_regex_from_proto(regex, proto):
    return re.findall(regex, str(proto))


def load_non_critical_topics():
    with open(_NON_CRITICAL_TOPICS_FILE) as f:
        _NON_CRITICAL_TOPICS = json.load(f)["list"]
        print(_NON_CRITICAL_TOPICS)


def does_contain_critical_topics(current_topics, non_crucial_list):
    for current_topic in current_topics:
        if is_topic_critical(current_topic, non_crucial_list):
            return True
    return False


def is_topic_critical(current_topic, non_crucial_list):
    for non_crucial in non_crucial_list:
        if non_crucial in current_topic:
            return False
    return True


def build_ad_removal_sync_operation(customer_id, ad_group_id, ad_id):
    resource_name = gAdsServiceWrapper.ad_group_ad_service.ad_group_ad_path(customer_id, ad_group_id, ad_id)
    ad_group_ad_op1 = gAdsServiceWrapper.client.get_type("AdGroupAdOperation")
    ad_group_ad_op1.remove = resource_name
    return ad_group_ad_op1


def send_bulk_mutate_request(customer_id, operations):
    # Issue a mutate request, setting partial_failure=True.
    request = gAdsServiceWrapper.client.get_type("MutateAdGroupAdsRequest")
    request.customer_id = customer_id
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
    return code != 0
    # [END handle_partial_failure_1]


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
            # Retrieve an instance of the GoogleAdsFailure class from the client
            failure_message = gAdsServiceWrapper.client.get_type("GoogleAdsFailure")
            # Parse the string into a GoogleAdsFailure message instance.
            # To access class-only methods on the message we retrieve its type.
            GoogleAdsFailure = type(failure_message)
            failure_object = GoogleAdsFailure.deserialize(error_detail.value)

            for error in failure_object.errors:
                # Construct and print a string that details which element in
                # the above ad_group_operations list failed (by index number)
                # as well as the error message and error code.
                print(
                    "A partial failure at index "
                    f"{error.location.field_path_elements[0].index} occurred "
                    f"\nError message: {error.message}\nError code: "
                    f"{error.error_code}"
                )
                index_array.append(error.location.field_path_elements[0].index)
                error_array.append({"error_message": error.message, "error_code": error.error_code})
    else:
        print(
            "All operations completed successfully. No partial failure "
            "to show."
        )

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
    print(
        f'Request with ID "{exception.request_id}" failed with status '
        f'"{exception.error.code().name}" and includes the following errors:'
    )
    for error in exception.failure.errors:
        print(f'\tError with message "{error.message}".')
        if error.location:
            for field_path_element in error.location.field_path_elements:
                print(f"\t\tOn field: {field_path_element.field_name}")
    sys.exit(1)


def delete_tables():
    bqServiceWrapper.delete_table(_PER_MCC_SUMMARY_TABLE_NAME)
    bqServiceWrapper.delete_table(_ADS_TO_REMOVE_TABLE_NAME)
    bqServiceWrapper.delete_table(_ALL_ACCOUNTS_TABLE_NAME)
    bqServiceWrapper.delete_table(_PER_ACCOUNT_SUMMARY_TABLE_NAME)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=(
            "Lists disapproved ads for a given top MCC"
        )
    )
    parser.add_argument(
        "-id",
        "--top_id",
        type=str,
        required=True,
        help="The Google Ads top mcc ID.",
    )
    parser.add_argument(
        "-p",
        "--parallel",
        action="store_true",
        help="Runs multiple accounts in parallel.",
    )
    parser.add_argument(
        "-rm",
        "--remove_ads",
        action="store_true",
        help="Should remove disapproved ads.",
    )
    parser.add_argument(
        "-ddb",
        "--delete_db",
        action="store_true",
        help="Delete DB tables.",
    )
    args = parser.parse_args()
    _REMOVE_ADS = args.remove_ads
    _PARALLEL_MODE = args.parallel
    load_non_critical_topics()
    current_session_id = str(uuid.uuid4())
    while _TRIES_LEFT > 0:
        _TRIES_LEFT -= 1
        try:
            bqServiceWrapper = BqServiceWrapper(_DS_ID)
            if args.delete_db:
                delete_tables()
                sys.exit(0)
            gAdsServiceWrapper = GAdsServiceWrapper(args.top_id)
            main(args.top_id)
            sys.exit(0)
        except GoogleAdsException as ex:
            handle_googleads_exception(ex)
