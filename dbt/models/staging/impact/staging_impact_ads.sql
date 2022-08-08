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

SELECT  ad.SRC:Id::VARCHAR AS id,
        metadata.SRC:networkKeyId::NUMBER AS network_key_id,
        metadata.SRC:namespace AS namespace,
        NULL AS advertiser_id,
        ad.SRC:CampaignId::VARCHAR AS campaign_id,
        NULL AS status,
        CASE
            WHEN ad.SRC:AdType::VARCHAR IS NULL THEN NULL
            WHEN LOWER(ad.SRC:AdType::VARCHAR) = 'not_applicable' then 'n/a'
            ELSE LOWER(ad.SRC:AdType::VARCHAR)
        END AS type,
        ad.SRC:Name::VARCHAR AS name,
        ad.SRC:Description::VARCHAR AS description,
        ad.SRC:LandingPage::VARCHAR AS advertiser_landing_page,
        '' AS tracking_link,
        NULL AS image_url,
        '' AS thumbnail_url,
        CURRENT_TIMESTAMP() AS created_at,
        CURRENT_TIMESTAMP() AS modified_at,
        ad.data_source AS data_source
  FROM {{ ref('raw_impact_ads') }} ad
            LEFT JOIN {{ ref('raw_metadata') }} metadata
        ON EQUAL_NULL({{ get_filename_from_path('ad.data_source') }}, {{ get_filename_from_path('metadata.data_source') }})
{% if is_incremental() %}
     WHERE ad.SRC:ingested_at >= DATEADD(HOUR, 2, CURRENT_TIMESTAMP())

{% endif %}
