{% macro create_namespaced_view(tableName) %}
     SELECT
         {{ dbt_utils.star(from=ref(tableName), except=["namespace"]) }}
       FROM {{ ref(tableName) }}
      WHERE
          namespace IN (
              SELECT allowed_namespace
              FROM {{ ref('role_permissions') }}
              WHERE role = CURRENT_ROLE()
          )
{% endmacro %}
