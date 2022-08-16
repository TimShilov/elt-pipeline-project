{{
  config(
    tags=['network1'],
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
    UUID_STRING() AS internal_id,
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
    actionListing.paid_datetime::TIMESTAMPNTZ AS paid_datetime,
    actionListing.paid_datetime_unixtime::NUMBER AS paid_datetime_unixtime,
    commission.SRC:MediaPartnerId::VARCHAR AS publisher_id,
    actionListing.terms_id::VARCHAR AS terms_id,
    actionListing.channel_id::VARCHAR AS channel_id,
    commission.SRC:CampaignId::VARCHAR AS campaign_id,
    commission.SRC:AdId::VARCHAR AS creative_id,
    actionListing.click_id::VARCHAR AS click_id,
    COALESCE(actionListing.event_type_id, commission.SRC:ActionTrackerId)::VARCHAR AS event_type_id,
    commission.SRC:Id::VARCHAR AS commission_id,
    actionListing.payment_id::VARCHAR AS payment_id,
    commission.SRC:Oid::VARCHAR AS order_id,
    IFF(commission.SRC:ReferringType = 'PPC', 'cpc', 'sale')::VARCHAR AS type,
    COALESCE(actionStatusDict.output, actionListing.status, 'approved')::VARCHAR AS status,
    1::INTEGER AS count,
    COALESCE(commission.SRC:Currency, actionListing.currency)::VARCHAR AS currency,
    (
        IFF(COALESCE(actionStatusDict.output, 'approved') = 'approved', commission.SRC:Amount::NUMBER, 0)
        +
        IFF(COALESCE(actionStatusDict.output, 'approved') = 'pending', commission.SRC:Amount::NUMBER, 0)
    )::NUMBER(15, 4) AS advertiser_revenue,
    IFF(COALESCE(actionStatusDict.output, 'approved') = 'rejected', commission.SRC:IntendedAmount::NUMBER, 0)::NUMBER(15, 4) AS disputed_advertiser_revenue,
    (
        IFF(COALESCE(actionStatusDict.output, 'approved') = 'approved', commission.SRC:Payout::NUMBER, 0)
        +
        IFF(COALESCE(actionStatusDict.output, 'approved') = 'pending', commission.SRC:Payout::NUMBER, 0)
    )::NUMBER(15, 4) AS publisher_commission,
    IFF(COALESCE(actionStatusDict.output, 'approved') = 'rejected', commission.SRC:IntendedPayout::NUMBER, 0)::NUMBER(15, 4) AS disputed_publisher_commission,
    NULL::NUMBER(15, 4) AS network_commission,
    NULL::NUMBER(15, 4) AS disputed_network_commission,
    NULL::NUMBER(15, 4) AS non_commissionable_advertiser_revenue,
    COALESCE(actionListing.event_type_id, commission.SRC:ActionTrackerName)::VARCHAR AS event_type_name,
    commission.SRC:PromoCode::VARCHAR AS promo_code,
    commission.SRC:CustomerCountry::VARCHAR AS country,
    commission.SRC:CustomerRegion::VARCHAR AS district,
    commission.SRC:CustomerCity::VARCHAR AS city,
    commission.SRC:CustomerPostCode::VARCHAR AS post_code,
    commission.SRC:IpAddress::VARCHAR AS ip_address,
    actionListing.device_type::VARCHAR AS device_type,
    actionListing.device_base_name::VARCHAR AS device_base_name,
    actionListing.device_model::VARCHAR AS device_model,
    actionListing.device_browser::VARCHAR AS device_browser,
    actionListing.device_browser_version::VARCHAR AS device_browser_version,
    actionListing.device_operating_system::VARCHAR AS device_operating_system,
    actionListing.device_operating_system_version::VARCHAR AS device_operating_system_version,
    actionListing.referral_url::VARCHAR AS referral_url,
    actionListing.referral_domain::VARCHAR AS referral_domain,
    actionListing.landing_page_url::VARCHAR AS landing_page_url,
    commission.SRC:SharedId::VARCHAR AS sub_id1,
    NULL::VARCHAR AS sub_id2,
    NULL::VARCHAR AS sub_id3,
    NULL::VARCHAR AS sub_id4,
    NULL::VARCHAR AS sub_id5,
    NULL::VARCHAR AS sub_id6,
    COALESCE(customerStatusDict.output, actionListing.repeat_customer)::BOOLEAN AS repeat_customer,
    NULL::BOOLEAN AS cross_device,
    COALESCE(NULLIF(commission.SRC:Note, ''), actionListing.details)::VARCHAR AS details,
    commission.SRC:EventCode::VARCHAR AS event_code,
    NULL::VARCHAR AS website_id,
    actionListing.customer_id::VARCHAR AS customer_id,
    (commission.SRC:ClientCost::NUMBER - ZEROIFNULL(commission.SRC:Payout::NUMBER))::NUMBER(15, 4) AS agency_commission,
    commission.SRC:SharedId::VARCHAR AS subaffiliate,
    CURRENT_TIMESTAMP() AS created_at,
    CURRENT_TIMESTAMP() AS modified_at,
    commission.data_source AS data_source
  FROM {{ ref('raw_network1_commissions') }} commission
  LEFT JOIN {{ ref('raw_metadata') }} metadata
        ON EQUAL_NULL(commission.data_source_filename, metadata.data_source_filename)
  LEFT JOIN {{ ref('staging_network1_advanced_action_listings') }} actionListing
       ON EQUAL_NULL(actionListing.commission_id, LOWER(commission.SRC:Id::VARCHAR))
            AND EQUAL_NULL(actionListing.network_key_id, metadata.SRC:networkKeyId::INTEGER)
  LEFT JOIN {{ ref('dict_network1_action_status') }} actionStatusDict
        ON EQUAL_NULL(actionStatusDict.input, LOWER(commission.SRC:State::VARCHAR))
  LEFT JOIN {{ ref('dict_network1_customer_status') }} customerStatusDict
        ON EQUAL_NULL(customerStatusDict.input, LOWER(commission.SRC:CustomerStatus::VARCHAR))
WHERE TRUE
{% if is_incremental() %}
    AND commission.ingested_at > DATEADD(HOUR, 2, CURRENT_TIMESTAMP())
{% endif %}
{{ dedupe_by_unique_key() }}
