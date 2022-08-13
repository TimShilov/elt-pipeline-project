{{
  config(
    materialized='incremental',
    unique_key = ['id', 'network_code'],
    on_schema_change='sync_all_columns',
    merge_update_columns = [
        'name',
        'description',
        'city',
        'district',
        'post_code',
        'country',
        'tags',
        'custom_group',
        'alternate_name',
        'type',
        'primary_email',
        'website',
        'type_override',
        'modified_at',
        'data_source'
    ],
  )
}}

SELECT publisher.SRC:Id::VARCHAR AS id,
       metadata.SRC:apiCode::VARCHAR AS network_code,
       COALESCE(publisher.SRC:Name, publisher.SRC:Website)::VARCHAR AS name,
       publisher.SRC:Description::VARCHAR AS description,
       publisher.SRC:City::VARCHAR AS city,
       publisher.SRC:CountryState::VARCHAR AS district,
       publisher.SRC:PostalCode::VARCHAR AS post_code,
       publisher.SRC:Country::VARCHAR AS country,
       NULL::VARCHAR AS tags,
       NULL::VARCHAR AS custom_group,
       NULL::VARCHAR AS alternate_name,
       channelDict.output AS type,
       NULL::VARCHAR AS type_override,
       publisher.SRC:Contact:EmailAddress::VARCHAR AS primary_email,
       publisher.SRC:Website::VARCHAR AS website,
       CURRENT_TIMESTAMP() AS created_at,
       CURRENT_TIMESTAMP() AS modified_at,
       publisher.data_source AS data_source
  FROM {{ ref('raw_impact_publishers') }} AS publisher
      LEFT JOIN {{ ref('raw_metadata') }} AS metadata
          ON EQUAL_NULL(publisher.data_source_filename, metadata.data_source_filename)
      LEFT JOIN {{ ref('dict_impact_channel') }} AS channelDict
          ON EQUAL_NULL(channelDict.input, publisher.SRC:PrimaryPromotionalMethod)
{% if is_incremental() %}
WHERE publisher.SRC:ingested_at >= DATEADD(HOUR, 2, CURRENT_TIMESTAMP())
{% endif %}
