{{
  config(materialized='view')
}}


{{ create_namespaced_view('affiliate_publisher_recruitment') }}
