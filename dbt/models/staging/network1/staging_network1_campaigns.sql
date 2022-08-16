{{
  config(
    tags=['network1'],
    materialized='incremental',
    unique_key = ['id', 'network_key_id'],
    on_schema_change='sync_all_columns',
    merge_update_columns = [
        'advertiser_id',
        'name',
        'status',
        'modified_at',
        'data_source'
    ],
  )
}}

SELECT publisher.campaign:CampaignId::VARCHAR AS id,
    metadata.SRC:networkKeyId::INTEGER AS network_key_id,
    metadata.SRC:namespace::VARCHAR AS namespace,
    0::INTEGER AS advertiser_id,
    publisher.campaign:CampaignName::VARCHAR AS name,
    publisher.SRC:RelationshipState::VARCHAR /* TODO: parseRelationshipStatus() */ AS status,
    CURRENT_TIMESTAMP() AS created_at,
    CURRENT_TIMESTAMP() AS modified_at,
    publisher.data_source AS data_source
FROM (SELECT publisher.*,
           c.value AS campaign
      FROM {{ ref('raw_network1_publishers') }} AS publisher,
           LATERAL FLATTEN(INPUT => SRC:Campaigns) AS c
     {% if is_incremental() %}
     WHERE publisher.SRC:ingested_at >= DATEADD(HOUR, 2, CURRENT_TIMESTAMP ())
     {% endif %}
     ) AS publisher
   LEFT JOIN {{ ref('raw_metadata') }} AS metadata
             ON EQUAL_NULL(publisher.data_source_filename, metadata.data_source_filename)
{{ dedupe_by_unique_key() }}
