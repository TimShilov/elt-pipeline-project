{{
  config(
    materialized='incremental',
    unique_key=['id', 'network_code'],
    on_schema_change='sync_all_columns',
    merge_update_columns = [
        'name',
        'description',
        'city',
        'district',
        'post_code',
        'country',
        'tags',
        'custom_group',
        'alternate_name',
        'type',
        'primary_email',
        'website',
        'modified_at',
        'data_source'
    ]
  )
}}


SELECT
    UUID_STRING() AS internal_id,
    {{ dbt_utils.star(from=ref('staging_impact_publishers')) }}
FROM {{ ref('staging_impact_publishers') }}

{% if is_incremental() %}
WHERE modified_at > DATEADD(HOUR, 2, CURRENT_TIMESTAMP())
{% endif %}
