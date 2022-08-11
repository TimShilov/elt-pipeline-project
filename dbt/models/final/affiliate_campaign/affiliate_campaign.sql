{{
  config(
    materialized='incremental',
    unique_key=['id', 'network_key_id'],
    on_schema_change='sync_all_columns',
    merge_update_columns = [
        'advertiser_id',
        'name',
        'status',
        'modified_at',
        'data_source'
    ]
  )
}}


SELECT
    UUID_STRING() AS affluent_id,
    {{ dbt_utils.star(from=ref('staging_impact_campaigns')) }}
FROM {{ ref('staging_impact_campaigns') }}

{% if is_incremental() %}
WHERE modified_at > DATEADD(HOUR, 2, CURRENT_TIMESTAMP())
{% endif %}
