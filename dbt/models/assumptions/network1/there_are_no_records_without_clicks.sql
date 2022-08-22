{{
  config(
    materialized='table',
  )
}}

SELECT COUNT(*) = 0 AS result
FROM {{ ref('raw_network1_clicks') }} click
WHERE click.SRC:UniqueClick IS NULL



