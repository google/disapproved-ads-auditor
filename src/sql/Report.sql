-- Copyright 2021 Google LLC
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     https://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- !/usr/bin/env python Disclaimer This is not an officially supported Google product. Copyright
-- 2021 Google LLC. This solution, including any related sample code or data, is made available on
-- an “as is,” “as available,” and “with all faults” basis, solely for illustrative purposes,
-- and without warranty or representation of any kind. This solution is experimental, unsupported
-- and provided solely for your convenience. Your use of it is subject to your agreements with
-- Google, as applicable, and may constitute a beta feature as defined under those agreements. To
-- the extent that you make any data available to Google in connection with your use of the
-- solution, you represent and warrant that you have all necessary and appropriate rights,
-- consents and permissions to permit Google to use and process that data. By using any portion of
-- this solution, you acknowledge, assume and accept all risks, known and unknown, associated with
-- its usage, including with respect to your deployment of any portion of this solution in your
-- systems, or usage in connection with your business, if at all.




--Temp table
WITH latest_account_scan AS
(
       SELECT *
       FROM   (
                        SELECT    all_accounts.*,
                                  disapproved_ads_count,
                                  Row_number() OVER(partition BY all_accounts.account_id ORDER BY finished_scans.timestamp DESC nulls last,all_accounts.timestamp DESC)=1 is_last_run
                        FROM      `spherestaging.google_3_strikes.allaccounts` all_accounts
                        LEFT JOIN `spherestaging.google_3_strikes.peraccountsummary` finished_scans
                        ON        all_accounts.account_id=finished_scans.account_id
                        AND       all_accounts.session_id=finished_scans.session_id ) account_scans
       WHERE  is_last_run )


-- totals of: account_scanned_last_24_hours, account_scanned_last_7_days, account_with_ads_to_be_removed, account_with_ads_removed,
-- total_disapproved_ads, removed_ads of the latest latest_account_scan
SELECT     count(1) total_account,
           count(
           CASE
                      WHEN cast(timestamp AS datetime)> date_add(current_datetime(),interval -2 day) THEN 1
           END) account_scanned_last_24_hours ,
           count(
           CASE
                      WHEN cast(timestamp AS datetime)> date_add(current_datetime(),interval -7 day) THEN 1
           END) account_scanned_last_7_days ,
           count(
           CASE
                      WHEN disapproved_ads>0 THEN 1
           END) account_with_ads_to_be_removed ,
           count(
           CASE
                      WHEN removed_ads>0 THEN 1
           END)                 account_with_ads_removed ,
           sum(disapproved_ads) total_disapproved_ads ,
           sum(removed_ads)     total_removed_ads
FROM       (
                    SELECT   session_id,
                             account_id account_id,
                             count(1)   disapproved_ads,
                             count(
                             CASE
                                      WHEN status='Removed' THEN 1
                             END) removed_ads
                    FROM     `spherestaging.google_3_strikes.adstoremove`
                    GROUP BY 1,
                             2 ) account_scan_agg
RIGHT JOIN latest_account_scan
ON         latest_account_scan.account_id = account_scan_agg.account_id
AND        latest_account_scan.session_id = account_scan_agg.session_id

