{{
  config(
    materialized='table'
  )
}}

SELECT
    campaignId,
    Description AS description,
    ThirdPartyServableAdCreativeHeight AS height,
    Id AS id,
    Labels AS labels,
    LandingPage AS landingPage,
    Language AS language,
    Name AS name,
    LinkText AS text,
    AdType AS type,
    ThirdPartyServableAdCreativeWidth AS width,
    data_source AS data_source
FROM {{ ref('staging_impact_ads') }}
