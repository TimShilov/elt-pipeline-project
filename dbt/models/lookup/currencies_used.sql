{{
  config(
    materialized='incremental',
    unique_key=['currency_code'],
    on_schema_change='sync_all_columns',
  )
}}

SELECT DISTINCT currency AS currency_code
  FROM {{ ref('affiliate_action') }} action
 WHERE currency IS NOT NULL
{% if is_incremental() %}
   AND action.modified_at > DATEADD(HOUR, -2, CURRENT_TIMESTAMP())
{% endif %}

