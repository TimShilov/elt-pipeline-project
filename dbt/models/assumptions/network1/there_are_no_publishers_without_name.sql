{{
  config(
    materialized='table',
  )
}}

SELECT COUNT(*) = 0 AS result
FROM {{ ref('raw_network1_publishers') }} publisher
WHERE publisher.SRC:Name IS NULL



