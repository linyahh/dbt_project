{% macro generate_user_growth_status(grain) %}

    {% if grain == 'day' %}
        {% set prev_expr = "date_sub(s.period_start, interval 1 day)" %}
        {% set spine_step = "interval 1 day" %}
    {% elif grain == 'week' %}
        {% set prev_expr = "date_sub(s.period_start, interval 1 week)" %}
        {% set spine_step = "interval 1 week" %}
    {% elif grain == 'month' %}
        {% set prev_expr = "date_sub(s.period_start, interval 1 month)" %}
        {% set spine_step = "interval 1 month" %}
    {% else %}
        {{ exceptions.raise_compiler_error("Invalid grain: " ~ grain) }}
    {% endif %}

    with raw_events as (

        select
            user_id,
            {{ period_start_expr('event_date', grain) }} as period_start,
            engagement_bucket
        from {{ ref('fct_events') }}

    ),

    activity as (

        select
            user_id,
            period_start,
            engagement_bucket as engagement_type
        from raw_events
        where engagement_bucket in ('pre_engagement', 'transaction')
        group by 1, 2, 3

        union all

        select
            user_id,
            period_start,
            'all' as engagement_type
        from raw_events
        group by 1, 2, 3

    ),

    activity_with_lag as (

        select
            user_id,
            engagement_type,
            period_start,
            lag(period_start) over (
                partition by user_id, engagement_type
                order by period_start
            ) as prev_active_period
        from activity

    ),

    first_activity_by_engagement as (

        select
            user_id,
            engagement_type,
            min(period_start) as first_period_start_by_engagement
        from activity
        group by 1, 2

    ),

    first_activity_overall as (

        select
            user_id,
            min(period_start) as first_period_start_overall
        from activity
        group by 1

    ),

    global_period_bounds as (

        select
            min(period_start) as min_period_start,
            max(period_start) as max_period_start
        from activity

    ),

    calendar_spine as (

        select period_start
        from global_period_bounds,
        unnest(
            generate_date_array(
                min_period_start,
                max_period_start,
                {{ spine_step }}
            )
        ) as period_start

    ),

    user_engagement_spine as (

        select
            fe.user_id,
            fe.engagement_type,
            c.period_start
        from first_activity_by_engagement fe
        join calendar_spine c
          on c.period_start >= fe.first_period_start_by_engagement

    ),

    last_activity_before_period as (

        select
            s.user_id,
            s.engagement_type,
            s.period_start,
            max(a.period_start) as last_active_period_before_current
        from user_engagement_spine s
        left join activity a
          on s.user_id = a.user_id
         and s.engagement_type = a.engagement_type
         and a.period_start < s.period_start
        group by 1, 2, 3

    ),

    spine_flags as (

        select
            s.user_id,
            s.engagement_type,
            s.period_start,

            case when curr.user_id is not null then 1 else 0 end as is_active_current,
            case when prev.user_id is not null then 1 else 0 end as is_active_prior,

            curr_lag.prev_active_period,
            lap.last_active_period_before_current,

            case
                when lap.last_active_period_before_current is not null then 1
                else 0
            end as has_any_prior_activity,

            case
                when curr_lag.prev_active_period is not null
                     and curr_lag.prev_active_period < {{ prev_expr }}
                    then 1
                else 0
            end as has_historical_before_prior

        from user_engagement_spine s
        left join activity curr
          on s.user_id = curr.user_id
         and s.engagement_type = curr.engagement_type
         and s.period_start = curr.period_start
        left join activity prev
          on s.user_id = prev.user_id
         and s.engagement_type = prev.engagement_type
         and prev.period_start = {{ prev_expr }}
        left join activity_with_lag curr_lag
          on s.user_id = curr_lag.user_id
         and s.engagement_type = curr_lag.engagement_type
         and s.period_start = curr_lag.period_start
        left join last_activity_before_period lap
          on s.user_id = lap.user_id
         and s.engagement_type = lap.engagement_type
         and s.period_start = lap.period_start

    ),

    classified as (

        select
            s.user_id,
            s.period_start,
            '{{ grain }}' as period_grain,
            s.engagement_type,

            s.is_active_current,
            s.is_active_prior,
            s.has_historical_before_prior,

            case
                when s.is_active_current = 1
                     and s.prev_active_period is null
                    then 'new'

                when s.is_active_current = 1
                     and s.prev_active_period = {{ prev_expr }}
                    then 'retained'

                when s.is_active_current = 1
                     and s.prev_active_period < {{ prev_expr }}
                    then 'resurrected'

                when s.is_active_current = 0
                     and s.has_any_prior_activity = 1
                    then 'churned'

                else 'other'
            end as growth_status,

            case
                when s.is_active_current = 1
                     and s.period_start = fo.first_period_start_overall
                    then true
                else false
            end as is_new_customer,

            case
                when s.is_active_current = 1
                     and s.period_start = fo.first_period_start_overall
                    then 'new_customer'
                else 'existing_customer'
            end as customer_status

        from spine_flags s
        left join first_activity_overall fo
          on s.user_id = fo.user_id

    )

    select
        user_id,
        period_start,
        period_grain,
        engagement_type,
        is_active_current,
        is_active_prior,
        has_historical_before_prior,
        growth_status,
        is_new_customer,
        customer_status
    from classified
    where growth_status != 'other'

{% endmacro %}