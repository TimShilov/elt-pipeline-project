{{
  config(
    materialized='table'
  )
}}

SELECT
    campaignId,
    description,
    height,
    id,
    labels,
    landingPage,
    language,
    name,
    text,
    type,
    width,
    CURRENT_TIMESTAMP() AS created_at,
    data_source AS data_source
FROM {{ ref('staging_impact_ads') }}
