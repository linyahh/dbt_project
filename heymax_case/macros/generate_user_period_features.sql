{% macro generate_user_period_features(grain) %}

    {% set valid_grains = ['day', 'week', 'month'] %}
    {% if grain not in valid_grains %}
        {{ exceptions.raise_compiler_error("Invalid grain: " ~ grain) }}
    {% endif %}

    with events_in_period as (

        select
            user_id,
            {{ period_start_expr('event_date', grain) }} as period_start,
            event_ts,
            event_type,
            transaction_category,
            miles_amount,
            platform,
            utm_source,
            country,
            gender
        from {{ ref('fct_events') }}

    ),

    -- distinct user/period combinations (the spine)
    user_periods as (

        select distinct user_id, period_start
        from events_in_period

    ),

    first_activity as (

        select
            user_id,
            min(period_start) as first_activity_period
        from user_periods
        group by 1

    ),

    period_rollup as (

        select
            e.user_id,
            e.period_start,
            '{{ grain }}' as period_grain,

            any_value(e.country) as country,
            any_value(e.gender) as gender,

            count(*) as event_count,
            sum(case when e.event_type = 'miles_earned' then coalesce(e.miles_amount, 0) else 0 end) as miles_earned,
            sum(case when e.event_type = 'miles_redeemed' then coalesce(e.miles_amount, 0) else 0 end) as miles_redeemed,

            -- first: earliest by event_ts
            array_agg(e.utm_source           ignore nulls order by e.event_ts)[safe_offset(0)] as first_utm_source,
            array_agg(e.event_type           ignore nulls order by e.event_ts)[safe_offset(0)] as first_event_type,
            array_agg(e.transaction_category ignore nulls order by e.event_ts)[safe_offset(0)] as first_transaction_category,
            array_agg(e.platform             ignore nulls order by e.event_ts)[safe_offset(0)] as first_platform,

            -- primary: most frequent value in the period (mode)
            approx_top_count(e.utm_source,           1)[safe_offset(0)].value as primary_utm_source,
            approx_top_count(e.event_type,           1)[safe_offset(0)].value as primary_event_type,
            approx_top_count(e.transaction_category, 1)[safe_offset(0)].value as primary_transaction_category,
            approx_top_count(e.platform,             1)[safe_offset(0)].value as primary_platform

        from events_in_period e
        group by 1, 2, 3

    )

    select
        p.period_start as reporting_date,
        p.user_id,
        p.period_grain,

        case
            when p.period_start = f.first_activity_period then 'new'
            else 'existing'
        end as user_type,

        p.country,
        p.gender,

        p.first_utm_source,
        p.first_event_type,
        p.first_transaction_category,
        p.first_platform,

        p.primary_utm_source,
        p.primary_event_type,
        p.primary_transaction_category,
        p.primary_platform,

        p.event_count,
        p.miles_earned,
        p.miles_redeemed

    from period_rollup p
    left join first_activity f
      on p.user_id = f.user_id

{% endmacro %}
