{{
  config(
    tags=['impact'],
    materialized='incremental',
    unique_key=['commission_id', 'network_key_id'],
    cluster_by='event_datetime',
    on_schema_change='sync_all_columns',
    merge_update_columns = [
        'event_datetime',
        'event_datetime_unixtime',
        'publisher_id',
        'type',
        'status',
        'currency',
        'publisher_commission',
        'disputed_publisher_commission',
        'modified_at',
        'data_source'
    ],
  )
}}


SELECT
    UUID_STRING() AS affluent_id,
    metadata.SRC:networkKeyId::INTEGER AS network_key_id,
    metadata.SRC:namespace::VARCHAR AS namespace,
    REPLACE(metadata.SRC:startDate, 'START_DATE=', '')::DATE AS event_datetime,
    DATE_PART(EPOCH_SECOND, REPLACE(metadata.SRC:startDate, 'START_DATE=', '')::DATE) AS event_datetime_unixtime,
    bonus.SRC:media_id::VARCHAR AS publisher_id,
    CONCAT_WS('_', 'Bonus', TO_CHAR(REPLACE(metadata.SRC:startDate, 'START_DATE=', '')::DATE, 'YYYYMMDD'), 'campaignId', bonus.SRC:media_id::VARCHAR)::VARCHAR AS commission_id,
    IFF(bonus.SRC:Bonus_Cost::NUMBER < 0, 'refunded_bonus', 'bonus')::VARCHAR AS type,
    IFF(bonus.SRC:Bonus_Cost::NUMBER < 0, 'refunded_bonus', 'bonus')::VARCHAR AS status,
    COALESCE(bonus.SRC:Currency, 'USD')::VARCHAR AS currency,
    IFF(bonus.SRC:Bonus_Cost::NUMBER >= 0, bonus.SRC:Bonus_Cost::NUMBER, NULL)::NUMBER(15, 4) AS publisher_commission,
    IFF(bonus.SRC:Bonus_Cost::NUMBER < 0, bonus.SRC:Bonus_Cost::NUMBER, NULL)::NUMBER(15, 4) AS disputed_publisher_commission,
    CURRENT_TIMESTAMP() AS created_at,
    CURRENT_TIMESTAMP() AS modified_at,
    bonus.data_source AS data_source
  FROM {{ ref('raw_impact_bonus') }} bonus
  LEFT JOIN {{ ref('raw_metadata') }} metadata
        ON EQUAL_NULL(bonus.data_source_filename, metadata.data_source_filename)
WHERE TRUE
{% if is_incremental() %}
    AND bonus.ingested_at > DATEADD(HOUR, 2, CURRENT_TIMESTAMP())
{% endif %}
{{ dedupe_by_unique_key() }}
