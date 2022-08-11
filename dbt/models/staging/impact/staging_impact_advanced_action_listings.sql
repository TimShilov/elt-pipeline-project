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
    commission.SRC:Action_Date::TIMESTAMPNTZ AS event_datetime,
    DATE_PART(EPOCH_SECOND, commission.SRC:Action_Date::TIMESTAMPNTZ) AS event_datetime_unixtime,
    commission.SRC:Referral_Date::TIMESTAMPNTZ AS click_datetime,
    DATE_PART(EPOCH_SECOND, commission.SRC:ReferringDate::TIMESTAMPNTZ) AS click_datetime_unixtime,
    NULL::TIMESTAMPNTZ AS processed_datetime,
    NULL::NUMBER AS processed_datetime_unixtime,
    commission.SRC:locking_date::TIMESTAMPNTZ AS finalized_datetime,
    DATE_PART(EPOCH_SECOND, commission.SRC:LockingDate::TIMESTAMPNTZ)::NUMBER AS finalized_datetime_unixtime,
    commission.SRC:locking_date::TIMESTAMPNTZ AS paid_datetime,
    NULL::NUMBER AS paid_datetime_unixtime,
    commission.SRC:MP_Id::VARCHAR AS publisher_id,
    NULL::VARCHAR AS terms_id,
    NULL::VARCHAR AS channel_id,
    NULL::VARCHAR AS campaign_id,
    commission.SRC:ad_id::VARCHAR AS creative_id,
    NULL::VARCHAR AS click_id,
    commission.SRC:AT_Id::VARCHAR AS event_type_id,
    commission.SRC:Action_Id::VARCHAR AS commission_id,
    NULL::VARCHAR AS payment_id,
    commission.SRC:OID::VARCHAR AS order_id,
    'sale'::VARCHAR AS type,
    COALESCE(actionStatusMap.output, 'approved')::VARCHAR AS status,
    0::INTEGER AS count, /* TODO */
    commission.SRC:Original_Currency::VARCHAR AS currency,
    0::NUMBER(15, 4) AS advertiser_revenue,/* TODO */
    0::NUMBER(15, 4) AS disputed_advertiser_revenue, /* TODO */
    0::NUMBER(15, 4) AS publisher_commission, /* TODO */
    0::NUMBER(15, 4) AS disputed_publisher_commission, /* TODO */
    0::NUMBER(15, 4) AS network_commission, /* TODO */
    0::NUMBER(15, 4) AS disputed_network_commission, /* TODO */
    0::NUMBER(15, 4) AS non_commissionable_advertiser_revenue, /* TODO */
    commission.SRC:Action_Tracker::VARCHAR AS event_type_name,
    commission.SRC:Promo_Code::VARCHAR AS promo_code,
    SPLIT_PART(commission.SRC:Geo_Location, ' - ', 1)::VARCHAR AS country,
    SPLIT_PART(commission.SRC:Geo_Location, ' - ', 2)::VARCHAR AS district,
    SPLIT_PART(commission.SRC:Geo_Location, ' - ', 3)::VARCHAR AS city,
    commission.SRC:Postcode::VARCHAR AS post_code,
    commission.SRC:IP_Address::VARCHAR AS ip_address,
    deviceTypeMap.output::VARCHAR AS device_type,
    NULL::VARCHAR AS device_base_name,
    commission.SRC:Device::VARCHAR AS device_model,
    TRIM(SPLIT_PART(commission.SRC:User_Agent, '.', 1))::VARCHAR AS device_browser,
    TRIM(SPLIT_PART(commission.SRC:User_Agent, '.', 2))::VARCHAR AS device_browser_version,
    TRIM(SPLIT_PART(commission.SRC:OS, '.', 1))::VARCHAR AS device_operating_system,
    TRIM(SPLIT_PART(commission.SRC:OS, '.', 2))::VARCHAR AS device_operating_system_version,
    commission.SRC:Referring_URL::VARCHAR AS referral_url,
    {{ dbt_utils.get_url_host(field='commission.SRC:Referring_URL') }}::VARCHAR AS referral_domain,
    NULL::VARCHAR AS landing_page_url,
    commission.SRC:SharedId::VARCHAR AS sub_id1,
    NULL::VARCHAR AS sub_id2,
    NULL::VARCHAR AS sub_id3,
    NULL::VARCHAR AS sub_id4,
    NULL::VARCHAR AS sub_id5,
    NULL::VARCHAR AS sub_id6,
    customerStatusMap.output::BOOLEAN AS repeat_customer,
    NULL::BOOLEAN AS cross_device,
    COALESCE(NULLIF(commission.SRC:Adv_String1, ''), NULLIF(commission.SRC:Adv_String2, ''), NULLIF(commission.SRC:Notes, ''))::VARCHAR AS details,
    commission.SRC:EventCode::VARCHAR AS event_code,
    NULL::VARCHAR AS website_id, /* TODO */
    commission.SRC:Customer_Id::VARCHAR AS customer_id,
    commission.SRC:ClientCost::NUMBER - COALESCE(commission.SRC:Payout, 0)::NUMBER AS agency_commission,
    commission.SRC:Shared_Id::VARCHAR AS subaffiliate,
    CURRENT_TIMESTAMP() AS created_at,
    CURRENT_TIMESTAMP() AS modified_at,
    commission.data_source AS data_source
  FROM {{ ref('raw_impact_advActionListing') }} commission
  LEFT JOIN {{ ref('raw_metadata') }} metadata
        ON EQUAL_NULL(commission.data_source_filename, metadata.data_source_filename)
  LEFT JOIN {{ ref('map_impact_action_status') }} actionStatusMap
        ON EQUAL_NULL(actionStatusMap.input, LOWER(commission.SRC:Status::VARCHAR))
  LEFT JOIN {{ ref('map_impact_device_type_infer') }} deviceTypeMap
        ON LOWER(commission.SRC:Device::VARCHAR) LIKE CONCAT('%', deviceTypeMap.input, '%')
  LEFT JOIN {{ ref('map_impact_customer_status') }} customerStatusMap
        ON EQUAL_NULL(customerStatusMap.input, LOWER(commission.SRC:Customer_Status::VARCHAR))
WHERE TRUE
{% if is_incremental() %}
    AND commission.ingested_at > DATEADD(HOUR, 2, CURRENT_TIMESTAMP())
{% endif %}
{{ dedupe_by_unique_key() }}