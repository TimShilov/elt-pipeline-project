{{
  config(materialized='view')
}}


SELECT
    {{ dbt_utils.star(from=ref('affiliate_publisher')) }}
FROM {{ ref('affiliate_publisher') }}
