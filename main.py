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
import sys
import json
from pathlib import Path
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException

_DEFAULT_PAGE_SIZE = 1000
_AUTO_REMOVE = False
_SUSPENSION_TOPICS = [each_string.lower() for each_string in ['Destination not working',
                                                              'Enabling dishonest behavior', 'Unapproved substances',
                                                              'Guns', 'Guns, gun parts and related products',
                                                              'Explosives', 'Other Weapons', 'Tobacco']]
_DISAPPROVED_ADS_AUDIT = Path('./disapproved_ads.json')


class ServiceWrapper:
    """Wraps GoogleAdsService API request"""

    def __init__(self, client, customer_id):
        self._client = client
        self._ga_service = client.get_service("GoogleAdsService")
        self._ad_group_ad_operation = client.get_type("AdGroupAdOperation")
        self._ad_request_type = client.get_type("SearchGoogleAdsRequest")
        self._customer_id = customer_id

    def get_rows(self, customer_id, query):
        request = self._ad_request_type
        request.customer_id = customer_id
        request.query = query
        request.page_size = _DEFAULT_PAGE_SIZE
        return self._ga_service.search(request=request)

    @property
    def ga_service(self):
        return self._ga_service

    @property
    def ad_group_ad_operation(self):
        return self._ad_group_ad_operation

    @property
    def ad_request_type(self):
        return self._ad_request_type

    @property
    def customer_id(self):
        return self._customer_id


def main():
    accounts_with_removed_ads = 0
    accounts_without_removed_ads = 0
    total_removed_ads = 0
    accounts = flat_all_accounts(serviceWrapper.customer_id, str(serviceWrapper.customer_id))
    for account in accounts:
        removed_ads_count = remove_disapproved_ads_for_account(account["id"])
        total_removed_ads += removed_ads_count
        if removed_ads_count > 0:
            accounts_with_removed_ads += 1
        else:
            accounts_without_removed_ads += 1
    print(f"\naccountsWithRemovedAds = %s, accountsWithoutRemovedAds = %s, totalRemovedAds = %s",
          str(accounts_with_removed_ads), str(accounts_without_removed_ads), str(total_removed_ads))


def flat_all_accounts(customer_id, hierarchy):
    """Returns a list {id, hierarchy} for all the descendant accounts of a given MCC account"""
    accounts = get_sub_accounts(False, customer_id, hierarchy)
    accounts.append({"id": customer_id, "hierarchy": hierarchy})
    sub_mccs = get_sub_accounts(True, customer_id, hierarchy)
    if len(sub_mccs) > 0:
        for sub_mcc in sub_mccs:
            accounts = accounts + flat_all_accounts(sub_mcc["id"], sub_mcc["hierarchy"])
    return accounts


def get_sub_accounts(is_mcc, customer_id, hierarchy):
    """Returns a list {id, hierarchy} for all the descendant accounts of a given MCC account
    which are mcc themselves = {is_mcc} """
    query = '''
    SELECT
      customer_client.descriptive_name,
      customer_client.id
    FROM
      customer_client
    WHERE
      customer_client.manager = ''' + str(is_mcc)

    accounts = []
    rows = serviceWrapper.get_rows(customer_id, query)
    for row in rows:
        customer_id_str = str(row.customer_client.id)
        if not customer_id_str == customer_id:
            accounts.append({"id": customer_id_str, "hierarchy": hierarchy + '_' +customer_id_str})
    return accounts


def remove_disapproved_ads_for_account(customer_id):
    """Remove all disapproved ads for a given customer id"""
    query = f"""
        SELECT
          customer.id,
          campaign.id,
          ad_group_ad.ad.id,
          ad_group_ad.ad.type,
          ad_group_ad.policy_summary.approval_status,
          ad_group_ad.policy_summary.policy_topic_entries
        FROM ad_group_ad
        WHERE
            ad_group_ad.policy_summary.approval_status = DISAPPROVED"""

    rows = serviceWrapper.get_rows(customer_id, query)
    disapproved_ads_count = 0
    print("Disapproved ads:")

    # Iterate over all ads in all rows returned and count disapproved ads.
    for row in rows:
        ad_group_ad = row.ad_group_ad
        ad = ad_group_ad.ad
        policy_summary = ad_group_ad.policy_summary

        print(
            f'Ad with ID "{ad.id}" and type "{ad.type_.name}" was '
            "disapproved with the following policy topic entries:"
        )

        # Display the policy topic entries related to the ad disapproval.
        for entry in policy_summary.policy_topic_entries:
            print(f'\ttopic: "{entry.topic}", type "{entry.type_.name}"')
            # Display the attributes and values that triggered the policy topic.
            for evidence in entry.evidences:
                for index, text in enumerate(evidence.text_list.texts):
                    print(f"\t\tevidence text[{index}]: {text}")
                    if entry.topic.lower() in _SUSPENSION_TOPICS:
                        disapproved_ads_count += 1
                        if _AUTO_REMOVE:
                            remove_ad(customer_id, ad_group_ad.ad_group.id, ad_group_ad.ad.id)

    print(f"\nNumber of relevant disapproved ads found: ", str(disapproved_ads_count))
    return disapproved_ads_count

    # with open(_DISAPPROVED_ADS_AUDIT, 'w') as f:
    #    json.dump(account_label_map, f, indent=2)
    # print('Json audit updated.')

def remove_ad(customer_id, ad_group_id, ad_id):
    """Removes the specified ad"""
    resource_name = serviceWrapper.ga_service.ad_group_ad_path(
        customer_id, ad_group_id, ad_id
    )
    serviceWrapper.ad_group_ad_operation.remove = resource_name

    ad_group_ad_response = serviceWrapper.ga_service.mutate_ad_group_ads(
        customer_id=customer_id, operations=[serviceWrapper.ad_group_ad_operation]
    )

    print(
        f"Removed ad group ad {ad_group_ad_response.results[0].resource_name}."
    )


if __name__ == "__main__":
    """ GoogleAdsClient will read the google-ads.yaml configuration file in the
     home directory if none is specified. """
google_ads_client = GoogleAdsClient.load_from_storage('./google-ads.yaml')
parser = argparse.ArgumentParser(
    description=(
        "Lists disapproved ads for a given top MCC"
    )
)
# The following argument(s) should be provided to run the example.
parser.add_argument(
    "-id",
    "--top_id",
    type=str,
    required=True,
    help="The Google Ads top mcc ID.",
)
parser.add_argument(
    "-rm",
    "--remove_ads",
    type=str,
    required=False,
    help="Should remove disapproved ads. Default: false",
)
args = parser.parse_args()
if args.remove_ads is not None:
    AUTO_REMOVE = args.remove_ads

try:
    serviceWrapper = ServiceWrapper(google_ads_client, args.top_id)
    main()
except GoogleAdsException as ex:
    print(
        f'Request with ID "{ex.request_id}" failed with status '
        f'"{ex.error.code().name}" and includes the following errors:'
    )
    for error in ex.failure.errors:
        print(f'\tError with message "{error.message}".')
        if error.location:
            for field_path_element in error.location.field_path_elements:
                print(f"\t\tOn field: {field_path_element.field_name}")
    sys.exit(1)

