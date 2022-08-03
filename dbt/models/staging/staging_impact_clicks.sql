{{
  config(
    materialized='incremental',
    unique_key='commissionId'
  )
}}

SELECT  metadata.SRC:networkKeyId AS network_key_id,
        metadata.SRC:namespace AS namespace,
        'click' AS actionType,
        metadata.SRC:clickDate::DATETIME AS eventDate,
        click.SRC:MediaId::INTEGER AS publisherId,
        click.SRC:MediaName::VARCHAR AS publisherName,
        click.SRC:UniqueClick::INTEGER AS clickCount,
        click.SRC:AdId::INTEGER AS adsId,
        click.SRC:campaignId::INTEGER AS campaignId,
        click.SRC:CustomerCountry::VARCHAR AS country,
        deviceTypeMap.output AS deviceType,
        click.SRC:CustomerRegion::VARCHAR AS district,
        DATE_PART(EPOCH_SECOND, metadata.SRC:clickDate::DATE) AS unixtime,
        {{
            dbt_utils.surrogate_key([
                'click.SRC:MediaId',
                'click.SRC:AdId',
                'click.SRC:CustomerCountry',
                'click.SRC:CustomerRegion',
                'click.SRC:DeviceType',
                'click.SRC:ReferringUrl'
            ])
        }} AS commissionId,
       click.data_source AS data_source,
       click.ingested_at AS ingested_at
  FROM {{ ref('raw_impact_clicks') }} click
  LEFT JOIN {{ ref('raw_metadata') }} metadata
        ON EQUAL_NULL({{ get_filename_from_path('click.data_source') }}, {{ get_filename_from_path('metadata.data_source') }})
  LEFT JOIN {{ ref('map_impact_device_type') }} deviceTypeMap
        ON EQUAL_NULL(deviceTypeMap.input, LOWER(click.SRC:DeviceType::VARCHAR))
WHERE click.SRC:UniqueClick IS NOT NULL
    {% if is_incremental() %}

    -- this filter will only be applied on an incremental run
    AND click.ingested_at > DATEADD(HOUR, 2, CURRENT_TIMESTAMP())

    {% endif %}
