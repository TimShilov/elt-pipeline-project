{{
  config(
    tags=['network1'],
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
    ],
  )
}}

SELECT
   metadata.SRC:networkKeyId::INTEGER AS network_key_id,
   metadata.SRC:namespace::VARCHAR AS namespace,
   TRIM(sku.SRC:ActionId) AS commission_id,
   0::INTEGER AS transaction_id,
   COALESCE(sku.SRC:SKU, sku.SRC:ItemName, sku.SRC:ActionId)::VARCHAR AS sku_id,
   COALESCE(sku.SRC:ItemName, sku.SRC:Product, sku.SRC:SKU)::VARCHAR AS product_name,
   sku.SRC:ItemzzzBrand::VARCHAR AS brand_name,
   {{ take_last_by_angle_bracket('sku.SRC:Category') }}::VARCHAR AS category1,
   {{ take_last_by_angle_bracket('sku.SRC:Subcategory') }}::VARCHAR AS category2,
   NULL::VARCHAR AS category_second_parent,
   NULL::VARCHAR AS unit_promo_code,
   sku.SRC:Quantity::INTEGER AS quantity,
   DIV0(sku.SRC:Revenue, sku.SRC:Quantity)::NUMBER(14, 4) AS unit_price,
   sku.SRC:Revenue::NUMBER(14, 4) AS total_price,
   DIV0(NULLIF(sku.SRC:Cost, ''), sku.SRC:Quantity)::NUMBER(14, 4) AS unit_commission,
   NULLIF(sku.SRC:Cost, '')::NUMBER(14, 4) AS total_commission,
   {{ take_last_by_angle_bracket('sku.SRC:CatalogCategory') }}::VARCHAR AS catalog_category,
   {{ take_last_by_angle_bracket('sku.SRC:CatalogSubcategory') }}::VARCHAR AS catalog_sub_category,
    CURRENT_TIMESTAMP() AS created_at,
    CURRENT_TIMESTAMP() AS modified_at,
    sku.data_source AS data_source
  FROM {{ ref('raw_network1_skus') }} sku
  LEFT JOIN {{ ref('raw_metadata') }} metadata
        ON EQUAL_NULL(sku.data_source_filename, metadata.data_source_filename)
WHERE NULLIF(TRIM(sku.SRC:ActionId), '') IS NOT NULL
{% if is_incremental() %}
    AND sku.ingested_at > DATEADD(HOUR, 2, CURRENT_TIMESTAMP())
{% endif %}
{{ dedupe_by_unique_key() }}
