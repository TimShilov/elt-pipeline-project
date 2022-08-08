{{
  config(
    materialized='table'
  )
}}

SELECT  metadata.SRC:networkKeyId AS network_key_id,
        metadata.SRC:namespace AS namespace,
        CASE
            WHEN ad.SRC:AdType::VARCHAR IS NULL THEN NULL
            WHEN LOWER(ad.SRC:AdType::VARCHAR) = 'not_applicable' then 'n/a'
            ELSE LOWER(ad.SRC:AdType::VARCHAR)
        END AS type,
        ad.SRC:CampaignId::VARCHAR AS campaignId,
        ad.SRC:Description::VARCHAR AS description,
        ad.SRC:Id::NUMBER AS id,
        ad.SRC:Labels::VARCHAR AS labels,
        ad.SRC:LandingPage::VARCHAR AS landingPage,
        ad.SRC:Language::VARCHAR AS language,
        ad.SRC:LinkText::VARCHAR AS text,
        ad.SRC:Name::VARCHAR AS name,
        NULLIF(ad.SRC:ThirdPartyServableAdCreativeHeight, '')::INTEGER AS height,
        NULLIF(ad.SRC:ThirdPartyServableAdCreativeWidth, '')::INTEGER AS width,
        ad.data_source AS data_source
  FROM {{ ref('raw_impact_ads') }} ad
            LEFT JOIN {{ ref('raw_metadata') }} metadata
        ON EQUAL_NULL({{ get_filename_from_path('ad.data_source') }}, {{ get_filename_from_path('metadata.data_source') }})
