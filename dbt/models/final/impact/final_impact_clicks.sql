{{
  config(
    materialized='incremental',
    unique_key='commissionId'
  )
}}

SELECT
    actionType,
    eventDate,
    unixtime,
    publisherId,
    publisherName,
    clickCount,
    adsId,
    campaignId,
    country,
    deviceType,
    district,
    commissionId,
    data_source,
    CURRENT_TIMESTAMP() AS created_at,
    CURRENT_TIMESTAMP() AS updated_at
FROM {{ ref('staging_impact_clicks') }}

{% if is_incremental() %}
-- this filter will only be applied on an incremental run
WHERE ingested_at > DATEADD(HOUR, 2, CURRENT_TIMESTAMP())
{% endif %}
