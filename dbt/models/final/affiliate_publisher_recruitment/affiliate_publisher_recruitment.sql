{{
  config(
    materialized='incremental',
    unique_key = ['campaign_id', 'publisher_id', 'network_key_id'],
    on_schema_change='sync_all_columns',
    merge_update_columns = [
        'apply_date',
        'join_date',
        'change_date',
        'reject_date',
        'terminate_date',
        'terms_id',
        'terms_name',
        'status',
        'modified_at',
        'data_source'
    ],
  )
}}


SELECT
    UUID_STRING() AS internal_id,
    {{ dbt_utils.star(from=ref('staging_network1_publisher_recruitments')) }}
FROM {{ ref('staging_network1_publisher_recruitments') }}

{% if is_incremental() %}
WHERE modified_at > DATEADD(HOUR, -2, CURRENT_TIMESTAMP())
{% endif %}
