{{
  config(
    materialized='incremental',
    unique_key=['network_key_id', 'commission_id', 'sku_id', 'transaction_id'],
    on_schema_change='sync_all_columns',
    merge_update_columns = [
        'product_name',
        'brand_name',
        'category1',
        'category2',
        'category_second_parent',
        'unit_promo_code',
        'quantity',
        'unit_price',
        'total_price',
        'unit_commission',
        'total_commission',
        'catalog_category',
        'catalog_sub_category',
        'modified_at',
        'data_source'
    ]
  )
}}


SELECT
    UUID_STRING() AS internal_id,
    {{ dbt_utils.star(from=ref('staging_network1_skus')) }}
FROM {{ ref('staging_network1_skus') }}

{% if is_incremental() %}
WHERE modified_at > DATEADD(HOUR, -2, CURRENT_TIMESTAMP())
{% endif %}
