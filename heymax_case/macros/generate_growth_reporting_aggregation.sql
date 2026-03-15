{% macro generate_growth_reporting_aggregation(
    source_model,
    dimensions=[],
    aggregation_sets=[],
    metrics=[],
    default_dimensions=['period_start', 'period_grain'],
    include_overall=true
) %}

    {% if dimensions | length == 0 %}
        {{ exceptions.raise_compiler_error("dimensions cannot be empty") }}
    {% endif %}

    {% if metrics | length == 0 %}
        {{ exceptions.raise_compiler_error("metrics cannot be empty") }}
    {% endif %}

    {# Validate dimension definitions #}
    {% for dim in dimensions %}
        {% if dim.get('name') is none or dim.get('expr') is none or dim.get('type') is none %}
            {{ exceptions.raise_compiler_error(
                "Each dimension must include 'name', 'expr', and 'type'"
            ) }}
        {% endif %}
    {% endfor %}

    {# Validate metric definitions #}
    {% for metric in metrics %}
        {% if metric.get('name') is none or metric.get('expr') is none %}
            {{ exceptions.raise_compiler_error(
                "Each metric must include 'name' and 'expr'"
            ) }}
        {% endif %}
    {% endfor %}

    {# Collect valid dimension names #}
    {% set valid_dimension_names = [] %}
    {% for dim in dimensions %}
        {% do valid_dimension_names.append(dim['name']) %}
    {% endfor %}

    {# Validate default dimensions #}
    {% for dim_name in default_dimensions %}
        {% if dim_name not in valid_dimension_names %}
            {{ exceptions.raise_compiler_error(
                "Invalid default dimension: " ~ dim_name
            ) }}
        {% endif %}
    {% endfor %}

    {# Build all requested aggregation sets #}
    {% set all_aggregation_sets = [] %}

    {% if include_overall %}
        {% do all_aggregation_sets.append([]) %}
    {% endif %}

    {% for agg_set in aggregation_sets %}
        {% do all_aggregation_sets.append(agg_set) %}
    {% endfor %}

    {% if all_aggregation_sets | length == 0 %}
        {{ exceptions.raise_compiler_error(
            "No aggregation sets provided. Set include_overall=true or pass aggregation_sets."
        ) }}
    {% endif %}

    {# Validate aggregation sets #}
    {% for agg_set in all_aggregation_sets %}
        {% for dim_name in agg_set %}
            {% if dim_name not in valid_dimension_names %}
                {{ exceptions.raise_compiler_error(
                    "Invalid dimension in aggregation_sets: " ~ dim_name
                ) }}
            {% endif %}
        {% endfor %}
    {% endfor %}

    with base as (

        select *
        from {{ ref(source_model) }}

    )

    {% for agg_set in all_aggregation_sets %}
        {% set selected_agg_dims = agg_set | unique | list %}
        {% set selected_dims = (default_dimensions + selected_agg_dims) | unique | list %}

        {# Human-readable aggregation label #}
        {% if selected_agg_dims | length == 0 %}
            {% set granularity_label = 'Overall' %}
        {% else %}
            {% set pretty_names = [] %}
            {% for dim_name in selected_agg_dims %}
                {% set pretty_name = dim_name | replace('_', ' ') | title %}
                {% do pretty_names.append(pretty_name) %}
            {% endfor %}
            {% set granularity_label = 'By ' ~ (pretty_names | join(' - ')) %}
        {% endif %}

        select
            {% for dim in dimensions %}
                {% if dim['name'] in selected_dims %}
                    {{ dim['expr'] }} as {{ dim['name'] }}
                {% else %}
                    cast(null as {{ dim['type'] }}) as {{ dim['name'] }}
                {% endif %}
                {% if not loop.last %},{% endif %}
            {% endfor %},
            '{{ granularity_label }}' as aggregation_granularity,
            {{ 'true' if selected_agg_dims | length == 0 else 'false' }} as is_overall
            {% for metric in metrics %}
            , {{ metric['expr'] }} as {{ metric['name'] }}
            {% endfor %}

        from base

        group by
            {% for i in range(1, dimensions | length + 3) %}
                {{ i }}{% if not loop.last %}, {% endif %}
            {% endfor %}

        {% if not loop.last %}
        union all
        {% endif %}

    {% endfor %}

{% endmacro %}