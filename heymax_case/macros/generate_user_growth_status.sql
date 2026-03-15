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

    -- one row per user / engagement_type / period
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

    -- use lag() to find the previous active period for each user/engagement row
    -- this avoids correlated subqueries which BigQuery does not support on CTEs
    activity_with_lag as (

        select
            user_id,
            engagement_type,
            period_start,
            lag(period_start) over (
                partition by user_id, engagement_type
                order by period_start
            ) as prev_active_period,
            -- count of active periods strictly before this one
            count(*) over (
                partition by user_id, engagement_type
                order by period_start
                rows between unbounded preceding and 1 preceding
            ) as prior_active_period_count
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

    -- for each user/engagement, the second most recent active period start
    -- used to detect if there's any activity before the prior period (for churned users
    -- who have no activity row at the prior period to join against)
    second_most_recent_activity as (

        select
            user_id,
            engagement_type,
            max(period_start) as second_most_recent_period
        from activity_with_lag
        where prior_active_period_count >= 1  -- at least one period before this one
        group by 1, 2

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

    spine_flags as (

        select
            s.user_id,
            s.engagement_type,
            s.period_start,

            case when curr.user_id is not null then 1 else 0 end as is_active_current,
            case when prev.user_id is not null then 1 else 0 end as is_active_prior,

            -- has_historical_before_prior: activity exists strictly before the prior period
            case
                when prev_lag.prev_active_period is not null
                    then 1
                when sma.second_most_recent_period is not null
                     and sma.second_most_recent_period < {{ prev_expr }}
                    then 1
                else 0
            end as has_historical_before_prior,

            -- has_any_prior_activity: user was active in any period before the current one
            -- used for the churned classification (not active now, but active before)
            case
                when prev.user_id is not null then 1
                when sma.second_most_recent_period is not null then 1
                else 0
            end as has_any_prior_activity

        from user_engagement_spine s
        left join activity curr
          on s.user_id = curr.user_id
         and s.engagement_type = curr.engagement_type
         and s.period_start = curr.period_start
        left join activity prev
          on s.user_id = prev.user_id
         and s.engagement_type = prev.engagement_type
         and prev.period_start = {{ prev_expr }}
        -- lag info for the prior period row (resurrected: was there activity before T-1?)
        left join activity_with_lag prev_lag
          on s.user_id = prev_lag.user_id
         and s.engagement_type = prev_lag.engagement_type
         and prev_lag.period_start = {{ prev_expr }}
        -- second most recent activity (churned: any history before T-1?)
        left join second_most_recent_activity sma
          on s.user_id = sma.user_id
         and s.engagement_type = sma.engagement_type

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
                -- New: active now, no prior period activity, no historical activity before prior
                -- is_active_prior = 0 AND has_historical_before_prior = 0 together mean
                -- this is definitively the user's first-ever active period for this engagement type
                when s.is_active_current = 1
                     and s.is_active_prior = 0
                     and s.has_historical_before_prior = 0
                    then 'new'

                -- Retained: active now, also active in prior period
                when s.is_active_current = 1
                     and s.is_active_prior = 1
                    then 'retained'

                -- Resurrected: active now, not active in prior period, but active before prior
                when s.is_active_current = 1
                     and s.is_active_prior = 0
                     and s.has_historical_before_prior = 1
                    then 'resurrected'

                -- Churned: not active now, but active in some prior period
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
