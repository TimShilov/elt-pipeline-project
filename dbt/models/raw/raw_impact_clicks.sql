{{
  config(
    materialized='from_external_stage',
    stage_name='GCS_STAGE',
    stage_storage_integration = 'AGENCY_STAGING_GCS',
    file_format = 'json_format'
  )
}}

SELECT $1::VARIANT AS SRC, metadata$filename AS data_source
  FROM {{ external_stage('/ir/clicks') }}
