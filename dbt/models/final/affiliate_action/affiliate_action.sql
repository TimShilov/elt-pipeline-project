{{
  config(
    materialized='incremental',
    unique_key=['commission_id', 'network_key_id'],
    cluster_by='event_datetime',
    on_schema_change='sync_all_columns',
    merge_update_columns = [
        'event_datetime',
        'event_datetime_unixtime',
        'click_datetime',
        'click_datetime_unixtime',
        'processed_datetime',
        'processed_datetime_unixtime',
        'finalized_datetime',
        'finalized_datetime_unixtime',
        'paid_datetime',
        'paid_datetime_unixtime',
        'publisher_id',
        'terms_id',
        'channel_id',
        'campaign_id',
        'creative_id',
        'click_id',
        'event_type_id',
        'payment_id',
        'order_id',
        'type',
        'status',
        'count',
        'currency',
        'advertiser_revenue',
        'disputed_advertiser_revenue',
        'publisher_commission',
        'disputed_publisher_commission',
        'network_commission',
        'disputed_network_commission',
        'non_commissionable_advertiser_revenue',
        'event_type_name',
        'promo_code',
        'country',
        'district',
        'city',
        'post_code',
        'ip_address',
        'device_type',
        'device_base_name',
        'device_model',
        'device_browser',
        'device_browser_version',
        'device_operating_system',
        'device_operating_system_version',
        'referral_url',
        'referral_domain',
        'landing_page_url',
        'sub_id1',
        'sub_id2',
        'sub_id3',
        'sub_id4',
        'sub_id5',
        'sub_id6',
        'repeat_customer',
        'cross_device',
        'details',
        'event_code',
        'website_id',
        'customer_id',
        'agency_commission',
        'subaffiliate',
        'modified_at',
        'data_source'
    ]
  )
}}

{# TODO: add this parameter after dbt-utils:0.9.0 release - source_column_name=none #}
{{ dbt_utils.union_relations(
    [ref('staging_impact_commissions'), ref('staging_impact_bonus')],
    where='modified_at > DATEADD(HOUR, 2, CURRENT_TIMESTAMP())' if is_incremental() else 'TRUE'
) }}


