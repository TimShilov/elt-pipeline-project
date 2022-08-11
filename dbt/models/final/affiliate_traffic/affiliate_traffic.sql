{{
  config(
    materialized='incremental',
    unique_key=['commission_id', 'network_key_id'],
    cluster_by='event_datetime',
    on_schema_change='sync_all_columns',
    merge_update_columns = [
        'event_datetime',
        'event_datetime_unixtime',
        'processed_datetime',
        'publisher_id',
        'terms_id',
        'channel_id',
        'campaign_id',
        'creative_id',
        'type',
        'click_count',
        'impression_count',
        'country',
        'district',
        'city',
        'post_code',
        'ip_address',
        'device_type',
        'device_base_name',
        'device_model',
        'device_browser',
        'device_browser_version',
        'device_operating_system',
        'device_operating_system_version',
        'referral_url',
        'referral_domain',
        'landing_page_url',
        'sub_id1',
        'sub_id2',
        'sub_id3',
        'sub_id4',
        'sub_id5',
        'sub_id6',
        'website_id',
        'modified_at',
        'data_source'
    ]
  )
}}


SELECT
    UUID_STRING() AS affluent_id,
    {{ dbt_utils.star(from=ref('staging_impact_clicks')) }}
FROM {{ ref('staging_impact_clicks') }}

{% if is_incremental() %}
WHERE modified_at > DATEADD(HOUR, 2, CURRENT_TIMESTAMP())
{% endif %}
UNION
SELECT
    UUID_STRING() AS affluent_id,
    {{ dbt_utils.star(from=ref('staging_impact_impressions')) }}
FROM {{ ref('staging_impact_impressions') }}

{% if is_incremental() %}
WHERE modified_at > DATEADD(HOUR, 2, CURRENT_TIMESTAMP())
{% endif %}

