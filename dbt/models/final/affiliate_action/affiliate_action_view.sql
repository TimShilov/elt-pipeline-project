{{
  config(materialized='view')
}}


{{ create_namespaced_view('affiliate_action') }}
