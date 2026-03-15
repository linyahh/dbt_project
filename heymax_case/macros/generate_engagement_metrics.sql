{% macro generate_engagement_metrics(grain) %}

    {% if grain == 'day' %}
        {% set activity_model = ref('int_user_activity_daily') %}
    {% elif grain == 'week' %}
        {% set activity_model = ref('int_user_activity_weekly') %}
    {% elif grain == 'month' %}
        {% set activity_model = ref('int_user_activity_monthly') %}
    {% else %}
        {{ exceptions.raise_compiler_error("Invalid grain: " ~ grain) }}
    {% endif %}

    with activity as (

        select *
        from {{ activity_model }}

    ),

    aggregated as (

        select
            period_start,
            engagement_type,
            count(distinct user_id) as active_users,
            sum(event_count) as total_events,
            sum(total_miles_amount) as total_miles_amount,
            safe_divide(sum(event_count), count(distinct user_id)) as events_per_active_user,
            safe_divide(sum(total_miles_amount), count(distinct user_id)) as miles_per_active_user,
            avg(event_count) as avg_events_per_user_period,
            avg(total_miles_amount) as avg_miles_per_user_period
        from activity
        group by 1,2

    )

    select
        period_start,
        '{{ grain }}' as period_grain,
        engagement_type,
        active_users,
        total_events,
        total_miles_amount,
        events_per_active_user,
        miles_per_active_user,
        avg_events_per_user_period,
        avg_miles_per_user_period
    from aggregated

{% endmacro %}