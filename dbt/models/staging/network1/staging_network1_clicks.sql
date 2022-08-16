{{
  config(
    tags=['network1'],
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
    ],
  )
}}

SELECT metadata.SRC:clickDate::DATETIME AS event_datetime,
    DATE_PART(EPOCH_SECOND, metadata.SRC:clickDate::DATE) AS event_datetime_unixtime,
    NULL AS processed_datetime,
    metadata.SRC:networkKeyId::INTEGER AS network_key_id,
    metadata.SRC:namespace::VARCHAR AS namespace,
    click.SRC:MediaId::INTEGER AS publisher_id,
    NULL AS terms_id,
    NULL AS channel_id,
    click.SRC:campaignId::INTEGER AS campaign_id,
    NULL AS creative_id,
    {{
        dbt_utils.surrogate_key([
            'metadata.SRC:clickDate',
            'click.SRC:MediaId',
            'click.SRC:AdId',
            'click.SRC:CustomerCountry',
            'click.SRC:CustomerRegion',
            'click.SRC:DeviceType',
            'click.SRC:ReferringUrl'
        ])
    }} AS commission_id,
    'click' AS type,
    click.SRC:UniqueClick::INTEGER AS click_count,
    0 AS impression_count,
    click.SRC:CustomerCountry::VARCHAR AS country,
    click.SRC:CustomerRegion::VARCHAR AS district,
    NULL AS city,
    NULL AS post_code,
    NULL AS ip_address,
    deviceTypeDict.output AS device_type,
    NULL AS device_base_name,
    NULL AS device_model,
    NULL AS device_browser,
    NULL AS device_browser_version,
    NULL AS device_operating_system,
    NULL AS device_operating_system_version,
    NULL AS referral_url,
    NULL AS referral_domain,
    NULL AS landing_page_url,
    NULL AS sub_id1,
    NULL AS sub_id2,
    NULL AS sub_id3,
    NULL AS sub_id4,
    NULL AS sub_id5,
    NULL AS sub_id6,
    NULL AS website_id,
    CURRENT_TIMESTAMP() AS created_at,
    CURRENT_TIMESTAMP() AS modified_at,
    click.data_source AS data_source
  FROM {{ ref('raw_network1_clicks') }} click
  LEFT JOIN {{ ref('raw_metadata') }} metadata
        ON EQUAL_NULL(click.data_source_filename, metadata.data_source_filename)
  LEFT JOIN {{ ref('dict_network1_device_type') }} deviceTypeDict
        ON EQUAL_NULL(deviceTypeDict.input, LOWER(click.SRC:DeviceType::VARCHAR))
WHERE click.SRC:UniqueClick IS NOT NULL
{% if is_incremental() %}
    AND click.ingested_at > DATEADD(HOUR, 2, CURRENT_TIMESTAMP())
{% endif %}
{{ dedupe_by_unique_key() }}
