{% macro take_last_by_angle_bracket(value) %}
    TRIM(SPLIT_PART({{ value }}, '>', -1))
{% endmacro %}
