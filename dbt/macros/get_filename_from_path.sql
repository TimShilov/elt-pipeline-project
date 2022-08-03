{% macro get_filename_from_path(filepath) %}
    split_part({{ filepath }}, '/', -1)
{% endmacro %}
