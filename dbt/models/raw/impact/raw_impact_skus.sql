{{
  config(
    materialized='from_external_stage',
    stage_name='GCS_STAGE',
    stage_storage_integration = 'AGENCY_STAGING_GCS',
    file_format = 'json_format'
  )
}}

SELECT $1::VARIANT AS SRC,
       CURRENT_TIMESTAMP() AS ingested_at,
       {{ get_datasource() }}
  FROM {{ external_stage('/ir/skus') }}
