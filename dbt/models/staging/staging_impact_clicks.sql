{{
  config(
    materialized='incremental',
    unique_key='commissionId'
  )
}}

SELECT 'click' AS actionType,
       REGEXP_SUBSTR(S.$1['@uri'], 'Start_Date=(\\d{4}-\\d{2}-\\d{2})',1,1,'c',1) AS eventDate,
        t.value:MediaId::INTEGER AS publisherId,
        t.value:MediaName::VARCHAR AS publisherName,
        t.value:UniqueClick::INTEGER AS clickCount,
        t.value:AdId::INTEGER AS adsId,
        t.value:campaignId::INTEGER AS campaignId,
        t.value:CustomerCountry::VARCHAR AS country,
        deviceTypeMap.output AS deviceType,
        t.value:CustomerRegion::VARCHAR AS district,
        DATE_PART(EPOCH_SECOND, REGEXP_SUBSTR(S.$1['@uri'], 'Start_Date=(\\d{4}-\\d{2}-\\d{2})',1,1,'c',1)::DATE) AS unixtime,
        {{
            dbt_utils.surrogate_key([
                't.value:MediaId',
                't.value:AdId',
                't.value:CustomerCountry',
                't.value:CustomerRegion',
                't.value:DeviceType',
                't.value:ReferringUrl'
            ])
        }} AS commissionId,
       S.data_source AS data_source,
       S.ingested_at AS ingested_at
  FROM {{ ref('raw_impact_clicks') }} AS S
          , TABLE(flatten(S.$1,'Records')) t
  JOIN {{ ref('map_impact_device_type') }} deviceTypeMap ON EQUAL_NULL(deviceTypeMap.input, LOWER(t.value:DeviceType::VARCHAR))
WHERE t.value:UniqueClick IS NOT NULL
    {% if is_incremental() %}

    -- this filter will only be applied on an incremental run
    AND S.ingested_at > DATEADD(HOUR, 2, CURRENT_TIMESTAMP())

    {% endif %}
