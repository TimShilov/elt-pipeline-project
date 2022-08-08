{% macro get_datasource() %}
    REPLACE(metadata$filename, '{{ var("storage_root_path") }}') AS data_source,
    split_part(REPLACE(metadata$filename, '{{ var("storage_root_path") }}'), '/', -1) AS data_source_filename
{% endmacro %}
