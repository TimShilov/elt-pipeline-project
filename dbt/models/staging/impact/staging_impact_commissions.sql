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
    ],
  )
}}
    

SELECT
    metadata.SRC:networkKeyId::INTEGER AS network_key_id,
    metadata.SRC:namespace::VARCHAR AS namespace,
    commission.SRC:EventDate::TIMESTAMPNTZ AS event_datetime,
    DATE_PART(EPOCH_SECOND, commission.SRC:EventDate::TIMESTAMPNTZ) AS event_datetime_unixtime,
    commission.SRC:ReferringDate::TIMESTAMPNTZ AS click_datetime,
    DATE_PART(EPOCH_SECOND, commission.SRC:ReferringDate::TIMESTAMPNTZ) AS click_datetime_unixtime,
    NULL::TIMESTAMPNTZ AS processed_datetime,
    NULL::NUMBER AS processed_datetime_unixtime,
    commission.SRC:LockingDate::TIMESTAMPNTZ AS finalized_datetime,
    DATE_PART(EPOCH_SECOND, commission.SRC:LockingDate::TIMESTAMPNTZ)::NUMBER AS finalized_datetime_unixtime,
    NULL::TIMESTAMPNTZ AS paid_datetime,
    NULL::NUMBER AS paid_datetime_unixtime,
    commission.SRC:MediaPartnerId::VARCHAR AS publisher_id,
    NULL::VARCHAR AS terms_id,
    NULL::VARCHAR AS channel_id,
    commission.SRC:CampaignId::VARCHAR AS campaign_id,
    commission.SRC:AdId::VARCHAR AS creative_id,
    NULL::VARCHAR AS click_id,
    commission.SRC:ActionTrackerId::VARCHAR AS event_type_id,
    commission.SRC:Id::VARCHAR AS commission_id,
    NULL::VARCHAR AS payment_id,
    commission.SRC:Oid::VARCHAR AS order_id,
    IFF(commission.SRC:ReferringType = 'PPC', 'cpc', 'sale')::VARCHAR AS type,
    COALESCE(actionStatusMap.output, 'approved')::VARCHAR AS status,
    0::INTEGER AS count, /* TODO */
    commission.SRC:Currency::VARCHAR AS currency,
    0::NUMBER(15, 4) AS advertiser_revenue,/* TODO */
    0::NUMBER(15, 4) AS disputed_advertiser_revenue, /* TODO */
    0::NUMBER(15, 4) AS publisher_commission, /* TODO */
    0::NUMBER(15, 4) AS disputed_publisher_commission, /* TODO */
    0::NUMBER(15, 4) AS network_commission, /* TODO */
    0::NUMBER(15, 4) AS disputed_network_commission, /* TODO */
    0::NUMBER(15, 4) AS non_commissionable_advertiser_revenue, /* TODO */
    commission.SRC:ActionTrackerName::VARCHAR AS event_type_name,
    commission.SRC:PromoCode::VARCHAR AS promo_code,
    commission.SRC:CustomerCountry::VARCHAR AS country,
    commission.SRC:CustomerRegion::VARCHAR AS district,
    commission.SRC:CustomerCity::VARCHAR AS city,
    commission.SRC:CustomerPostCode::VARCHAR AS post_code,
    commission.SRC:IpAddress::VARCHAR AS ip_address,
    NULL::VARCHAR AS device_type, /* TODO */
    NULL::VARCHAR AS device_base_name, /* TODO */
    NULL::VARCHAR AS device_model, /* TODO */
    NULL::VARCHAR AS device_browser, /* TODO */
    NULL::VARCHAR AS device_browser_version, /* TODO */
    NULL::VARCHAR AS device_operating_system, /* TODO */
    NULL::VARCHAR AS device_operating_system_version, /* TODO */
    NULL::VARCHAR AS referral_url, /* TODO */
    NULL::VARCHAR AS referral_domain, /* TODO */
    NULL::VARCHAR AS landing_page_url, /* TODO */
    commission.SRC:SharedId::VARCHAR AS sub_id1,
    NULL::VARCHAR AS sub_id2,
    NULL::VARCHAR AS sub_id3,
    NULL::VARCHAR AS sub_id4,
    NULL::VARCHAR AS sub_id5,
    NULL::VARCHAR AS sub_id6,
    customerStatusMap.output::BOOLEAN AS repeat_customer,
    NULL::BOOLEAN AS cross_device, /* TODO */
    commission.SRC:Note::VARCHAR AS details,
    commission.SRC:EventCode::VARCHAR AS event_code,
    NULL::VARCHAR AS website_id, /* TODO */
    NULL::VARCHAR AS customer_id, /* TODO */
    commission.SRC:ClientCost::NUMBER - COALESCE(commission.SRC:Payout, 0)::NUMBER AS agency_commission,
    commission.SRC:SharedId::VARCHAR AS subaffiliate,
    CURRENT_TIMESTAMP() AS created_at,
    CURRENT_TIMESTAMP() AS modified_at,
    commission.data_source AS data_source
  FROM {{ ref('raw_impact_commissions') }} commission
  LEFT JOIN {{ ref('raw_metadata') }} metadata
        ON EQUAL_NULL(commission.data_source_filename, metadata.data_source_filename)
  LEFT JOIN {{ ref('map_impact_action_status') }} actionStatusMap
        ON EQUAL_NULL(actionStatusMap.input, LOWER(commission.SRC:State::VARCHAR))
  LEFT JOIN {{ ref('map_impact_customer_status') }} customerStatusMap
        ON EQUAL_NULL(customerStatusMap.input, LOWER(commission.SRC:CustomerStatus::VARCHAR))
WHERE TRUE
{% if is_incremental() %}
    AND commission.ingested_at > DATEADD(HOUR, 2, CURRENT_TIMESTAMP())
{% endif %}
{{ dedupe_by_unique_key() }}
