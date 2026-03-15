{% macro period_start_expr(date_col, grain) %}
    {% if grain == 'day' %}
        {{ date_col }}
    {% elif grain == 'week' %}
        date_trunc({{ date_col }}, week)
    {% elif grain == 'month' %}
        date_trunc({{ date_col }}, month)
    {% else %}
        {{ exceptions.raise_compiler_error("Invalid grain: " ~ grain) }}
    {% endif %}
{% endmacro %}