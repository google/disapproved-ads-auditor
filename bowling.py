#!/usr/bin/env python
# Copyright 2018 Google LLC
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
"""This illustrates how to retrieve disapproved ads in a given campaign."""
 
 
import argparse
import sys
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
 
 
_DEFAULT_PAGE_SIZE = 1000
_AUTO_REMOVE = false
_SUSPENSION_TOPICS = ['Destination not working', 'Enabling dishonest behavior', 'Unapproved substances', 'Guns', 'Guns, gun parts and related products', 'Explosives', 'Other Weapons', 'Tobacco' ]
_DISAPPROVED_ADS_AUDIT = Path('./disapproved_ads.json')
 
 
class Runner(object):
    def __init__(self, client, customer_id):
        self._client = client
        self._ga_service = client.get_service("GoogleAdsService")
        self._ad_group_ad_operation = client.get_type("AdGroupAdOperation")
        self._ad_request_type= client.get_type("SearchGoogleAdsRequest")
        self._customer_id = customer_id
    
    def main():
        accountsWithRemovedAds = 0
        accountsWithoutRemovedAds = 0
        totalRemovedAds = 0
        accounts = _flat_all_accounts(self._customer_id, str(self._customer_id))
        for account in accounts:
            remvoedAdsCount = _removeDisapprovedAdsForCustomer(account)
            if (removedAdsCount > 0):
                accountsWithRemovedAds++
                totalRemovedAds += remvoedAdsCount
            else:    
                accountsWithoutRemovedAds++
                
        print(f"\naccountsWithRemovedAds = %s, accountsWithoutRemovedAds = %s, totalRemovedAds = %s", str(accountsWithRemovedAds) ,str(accountsWithoutRemovedAds) str(totalRemovedAds))
    
    #    """Gets all sub accounts' IDs under the top MCC."""
    def _get_sub_accounts(parent_customer_id, hierarchy):
        accounts = []
        query = '''
        SELECT
          customer_client.descriptive_name,
          customer_client.id
        FROM
          customer_client
        WHERE
          customer_client.manager = False
        '''
        request = client.get_type("SearchGoogleAdsRequest")
        request.customer_id = parent_customer_id
        request.query = query
        request.page_size = _DEFAULT_PAGE_SIZE
        rows = self._ga_service.search(request=request)
        
        for batch in rows:
            for row in batch.results:
                accounts.append({ id: row.customer_client.id, hierarchy: hierarchy+'_'+row.customer_client.id))
        return accounts
    
    #    """Gets all MCC accounts' IDs under the top MCC."""
    def _get_sub_mcc(parent_customer_id, hierarchy)
        accounts = []
        query = '''
        SELECT
          customer_client.id
        FROM
          customer_client
        WHERE
          customer_client.manager = True
        '''
        request = self._ad_request_type
        request.customer_id = parent_customer_id
        request.query = query
        request.page_size = _DEFAULT_PAGE_SIZE
        rows = self._ga_service.search(request=request)
        for batch in rows:
            for row in batch.results:
                accounts.append({ id: row.customer_client.id, hierarchy: hierarchy+'_'+row.customer_client.id))
        return accounts
            
    def _flat_all_accounts(customer_id, hierarchy):
        accounts = _get_sub_accounts(customer_id, hierarchy)
        subMccs = _get_sub_mcc(customer_id, hierarchy)
        if (subMcc.length > 0):
            for subMcc in subMccs:
                accounts.append(_flat_all_accounts(subMcc.id, subMcc.hierarchy)
        return accounts
 
    def _removeDisapprovedAdsForCustomer(customer_id):
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
              AND ad_group_ad.policy_summary.approval_status = DISAPPROVED """
    #          AND ad_group_ad.policy_summary.policy_topic_entries CONTAINS_ANY ['Destination not working', 'Enabling dishonest behavior', 'Unapproved substances', 'Guns', 'Guns, gun parts and related products', 'Explosives', 'Other Weapons', 'Tobacco' ] """
    
    
        request = self._ga_request_type
        request.customer_id = customer_id
        request.query = query
        request.page_size = _DEFAULT_PAGE_SIZE
        results = self._ga_service.search(request=request)
    
        disapproved_ads_count = 0
    #    disapproved_enum = client.enums.PolicyApprovalStatusEnum.DISAPPROVED
    
        print("Disapproved ads:")
    
        # Iterate over all ads in all rows returned and count disapproved ads.
        for row in results:
            ad_group_ad = row.ad_group_ad
            ad = ad_group_ad.ad
            policy_summary = ad_group_ad.policy_summary
 
            print(
                f'Ad with ID "{ad.id}" and type "{ad.type_.name}" was '
                "disapproved with the following policy topic entries:"
            )
    
            # Display the policy topic entries related to the ad disapproval.
            for entry in policy_summary.policy_topic_entries:
    	        if entry.type_.name in _SUSPENSION_TOPICS:
    	            	print(f'\ttopic: "{entry.topic}", type "{entry.type_.name}"')
    	            	if (_AUTO_REMOVE):
    	            	    _remove_ad(customer_id,,ad_group_ad.ad.id)
    	            	    disapproved_ads_count++
    
    		# Display the attributes and values that triggered the policy
    		# topic.
    		for evidence in entry.evidences:
    		    for index, text in enumerate(evidence.text_list.texts):
    		        print(f"\t\tevidence text[{index}]: {text}")
    
        print(f"\nNumber of disapproved ads found: %s", str(disapproved_ads_count))
        return disapproved_ads_count
        #with open(_DISAPPROVED_ADS_AUDIT, 'w') as f:
        #    json.dump(account_label_map, f, indent=2)    
        #print('Map updated.')
        
        
    def _remove_ad(customer_id, ad_group_id, ad_id):
        resource_name = self_ag_service.ad_group_ad_path(
            customer_id, ad_group_id, ad_id
        )
        ad_group_ad_operation.remove = resource_name
 
        ad_group_ad_response = ad_group_ad_service.mutate_ad_group_ads(
            customer_id=customer_id, operations=[self._ad_group_ad_operation]
        )
 
        print(
            f"Removed ad group ad {ad_group_ad_response.results[0].resource_name}."
        )
 
 
if __name__ == "__main__":
    # GoogleAdsClient will read the google-ads.yaml configuration file in the
    # home directory if none is specified.
    googleads_client = GoogleAdsClient.load_from_storage(version="v8")
 
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
        help="Should remove disapproved ads. Defualt: false",
    )
 
    args = parser.parse_args()
 
    if (args.remove_ads):
        AUTO_REMOVE = args.remove_ads
 
    try:
        Runner(googleads_client, args.top_id).main()
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
