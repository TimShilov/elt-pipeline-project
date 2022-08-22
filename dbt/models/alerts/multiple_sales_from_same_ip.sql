{{
  config(
    materialized='table',
  )
}}

SELECT network_key_id,
       namespace,
       DATE(event_datetime) AS DATE,
       publisher_id,
       ip_address,
       COUNT(*) AS sale_count
  FROM {{ ref('affiliate_action') }}
 WHERE TYPE = 'sale'
   AND NULLIF(IP_ADDRESS, '') IS NOT NULL
   AND DATE(event_datetime) BETWEEN DATEADD(WEEK, -2, CURRENT_TIMESTAMP) AND CURRENT_TIMESTAMP
 GROUP BY network_key_id, namespace, DATE, publisher_id, ip_address
HAVING sale_count > 2

