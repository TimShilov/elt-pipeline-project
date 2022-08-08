{{
  config(
    materialized='incremental',
    unique_key = ['id', 'network_key_id'],
    on_schema_change='sync_all_columns',
    merge_update_columns = [
        'advertiser_id',
        'campaign_id',
        'status',
        'type',
        'name',
        'description',
        'advertiser_landing_page',
        'tracking_link',
        'image_url',
        'thumbnail_url',
        'modified_at',
        'data_source',
    ],
  )
}}

SELECT *
FROM {{ ref('staging_impact_ads') }} AS impact_ad
{% if is_incremental() %}
  WHERE impact_ad.modified_at >= DATEADD(HOUR, 2, CURRENT_TIMESTAMP())

{% endif %}
