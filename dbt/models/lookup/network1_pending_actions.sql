{{
  config(
    materialized='table',
  )
}}

SELECT *
  FROM {{ ref('affiliate_action') }} action
 WHERE status = 'pending' AND DATA_SOURCE LIKE '%/ir/%'

