{{
  config(
    materialized='table'
  )
}}

SELECT CASE
            WHEN t.value:AdType::VARCHAR IS NULL THEN NULL
            WHEN LOWER(t.value:AdType::VARCHAR) = 'not_applicable' then 'n/a'
            ELSE LOWER(t.value:AdType::VARCHAR)
        END AS type,
        t.value:CampaignId::VARCHAR AS campaignId,
        t.value:Description::VARCHAR AS description,
        t.value:Id::NUMBER AS id,
        t.value:Labels::VARCHAR AS labels,
        t.value:LandingPage::VARCHAR AS landingPage,
        t.value:Language::VARCHAR AS language,
        t.value:LinkText::VARCHAR AS text,
        t.value:Name::VARCHAR AS name,
        NULLIF(t.value:ThirdPartyServableAdCreativeHeight, '')::INTEGER AS height,
        NULLIF(t.value:ThirdPartyServableAdCreativeWidth, '')::INTEGER AS width,
        S.data_source AS data_source
  FROM {{ ref('raw_impact_ads') }} AS S
          , TABLE(flatten(S.$1,'Ads')) t
