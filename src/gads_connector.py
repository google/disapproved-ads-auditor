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


from google.ads.googleads.client import GoogleAdsClient

GOOGLE_ADS_YAML = './secret_keys/google-ads.yaml'
_TIMEOUT_MILLIS = 1000 * 15


class GAdsServiceWrapper:
    """Wraps GoogleAdsService API request"""

    @property
    def client(self):
        return self._client

    @property
    def ga_service(self):
        return self._ga_service

    @property
    def customer_id(self):
        return self._customer_id

    @property
    def ad_group_ad_service(self):
        return self._ad_group_ad_service

    def __init__(self, customer_id):
        """ GoogleAdsClient will read the google-ads.yaml configuration file in the
         home directory if none is specified. """
        self._client = GoogleAdsClient.load_from_storage(GOOGLE_ADS_YAML)
        self._ga_service = self._client.get_service("GoogleAdsService")
        self._ad_group_ad_service = self._client.get_service("AdGroupAdService")
        self._customer_id = customer_id

    def get_stream_of_rows(self, customer_id, query):
        """Returns a stream of results from GAds API"""
        search_request = self._client.get_type("SearchGoogleAdsStreamRequest")
        search_request.customer_id = customer_id
        search_request.query = query
        return self._ga_service.search_stream(request=search_request)

    def get_sub_accounts(self, is_mcc, customer_id, hierarchy):
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
        rows = self.get_stream_of_rows(customer_id, query)
        for batch in rows:
            for row in batch.results:
                customer_id_str = str(row.customer_client.id)
                if not customer_id_str == customer_id:
                    accounts.append({"account_id": customer_id_str,
                                     "hierarchy": hierarchy + '_' + customer_id_str})
        return accounts

    def get_disapproved_ads_for_account(self, account_id):
        """Returns disapproved ads for account"""
        query = """
            SELECT
              customer.id,
              campaign.id,
              ad_group_ad.ad.type,
              ad_group_ad.ad.text_ad.headline,
              ad_group_ad.ad.text_ad.description1,
              ad_group_ad.ad.text_ad.description2,
              ad_group_ad.ad.expanded_text_ad.description,
              ad_group_ad.ad.expanded_text_ad.description2,
              ad_group_ad.ad.expanded_text_ad.headline_part1,
              ad_group_ad.ad.expanded_text_ad.headline_part2,
              ad_group_ad.ad.expanded_text_ad.headline_part3,
              ad_group_ad.ad.responsive_search_ad.headlines,
              ad_group_ad.ad.responsive_search_ad.descriptions,
              ad_group_ad.ad.responsive_search_ad.path1,
              ad_group_ad.ad.responsive_search_ad.path2,
              ad_group_ad.ad.id,
              ad_group_ad.ad.final_urls,
              ad_group_ad.ad.type,
              ad_group_ad.ad_group,
              ad_group_ad.policy_summary.approval_status,
              ad_group_ad.policy_summary.policy_topic_entries
            FROM ad_group_ad
            WHERE         
                ad_group_ad.policy_summary.approval_status = DISAPPROVED
                AND ad_group_ad.status != REMOVED  """
        return self.get_stream_of_rows(account_id, query)
