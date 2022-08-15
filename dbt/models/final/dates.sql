{{
    config(materialized = "table")
}}

{{ dbt_utils.date_spine(
    datepart="day",
    start_date="cast('2010-01-01' AS date)",
    end_date="cast('2050-01-01' AS date)"
   )
}}
