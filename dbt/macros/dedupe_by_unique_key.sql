{% macro dedupe_by_unique_key() %}
    QUALIFY ROW_NUMBER()
       OVER (
          PARTITION BY {{ config.get('unique_key')|join(', ') }}
          ORDER BY metadata.SRC:timeCreated::DATETIME
       ) = 1
{% endmacro %}
