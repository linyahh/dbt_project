{% macro generate_growth_user_features(grain) %}

    {% if grain == 'day' %}
        {% set growth_model = ref('int_user_growth_status_daily') %}
    {% elif grain == 'week' %}
        {% set growth_model = ref('int_user_growth_status_weekly') %}
    {% elif grain == 'month' %}
        {% set growth_model = ref('int_user_growth_status_monthly') %}
    {% else %}
        {{ exceptions.raise_compiler_error("Invalid grain: " ~ grain) }}
    {% endif %}

    with growth_status as (

        select *
        from {{ growth_model }}

    ),

    users as (

        select *
        from {{ ref('dim_user') }}

    ),

    events_base as (

        select
            user_id,
            {{ period_start_expr('event_date', grain) }} as period_start,
            event_ts,
            event_type,
            transaction_category,
            platform,
            utm_source,
            engagement_bucket,
            miles_amount
        from {{ ref('fct_events') }}

    ),

    events_filtered as (

        select
            g.user_id,
            g.period_start,
            g.engagement_type,
            e.event_ts,
            e.event_type,
            e.transaction_category,
            e.platform,
            e.utm_source,
            e.engagement_bucket,
            e.miles_amount
        from growth_status g
        join events_base e
          on g.user_id = e.user_id
         and g.period_start = e.period_start
        where
            g.engagement_type = 'all'
            or (g.engagement_type = 'pre_engagement' and e.engagement_bucket = 'pre_engagement')
            or (g.engagement_type = 'transaction' and e.engagement_bucket = 'transaction')

    ),

    period_metrics as (

        select
            user_id,
            period_start,
            engagement_type,
            count(*) as period_event_count,
            sum(coalesce(miles_amount, 0)) as period_miles_amount,
            max(case when engagement_bucket = 'transaction' then 1 else 0 end) as has_transaction_event,
            max(case when engagement_bucket = 'pre_engagement' then 1 else 0 end) as has_pre_engagement_event
        from events_filtered
        group by 1,2,3

    ),

    first_platform as (

        select user_id, period_start, engagement_type, platform as first_platform_in_period
        from (
            select
                user_id,
                period_start,
                engagement_type,
                platform,
                row_number() over (
                    partition by user_id, period_start, engagement_type
                    order by event_ts asc, platform
                ) as rn
            from events_filtered
            where platform is not null
        )
        where rn = 1

    ),

    first_utm as (

        select user_id, period_start, engagement_type, utm_source as first_utm_source_in_period
        from (
            select
                user_id,
                period_start,
                engagement_type,
                utm_source,
                row_number() over (
                    partition by user_id, period_start, engagement_type
                    order by event_ts asc, utm_source
                ) as rn
            from events_filtered
            where utm_source is not null
        )
        where rn = 1

    ),

    first_event_type as (

        select user_id, period_start, engagement_type, event_type as first_event_type_in_period
        from (
            select
                user_id,
                period_start,
                engagement_type,
                event_type,
                row_number() over (
                    partition by user_id, period_start, engagement_type
                    order by event_ts asc, event_type
                ) as rn
            from events_filtered
            where event_type is not null
        )
        where rn = 1

    ),

    first_transaction_category as (

        select user_id, period_start, engagement_type, transaction_category as first_transaction_category_in_period
        from (
            select
                user_id,
                period_start,
                engagement_type,
                transaction_category,
                row_number() over (
                    partition by user_id, period_start, engagement_type
                    order by event_ts asc, transaction_category
                ) as rn
            from events_filtered
            where transaction_category is not null
        )
        where rn = 1

    ),

    primary_platform as (

        select user_id, period_start, engagement_type, platform as primary_platform_in_period
        from (
            select
                user_id,
                period_start,
                engagement_type,
                platform,
                row_number() over (
                    partition by user_id, period_start, engagement_type
                    order by count(*) desc, platform
                ) as rn
            from events_filtered
            where platform is not null
            group by 1,2,3,4
        )
        where rn = 1

    ),

    primary_utm as (

        select user_id, period_start, engagement_type, utm_source as primary_utm_source_in_period
        from (
            select
                user_id,
                period_start,
                engagement_type,
                utm_source,
                row_number() over (
                    partition by user_id, period_start, engagement_type
                    order by count(*) desc, utm_source
                ) as rn
            from events_filtered
            where utm_source is not null
            group by 1,2,3,4
        )
        where rn = 1

    ),

    primary_event_type as (

        select user_id, period_start, engagement_type, event_type as primary_event_type_in_period
        from (
            select
                user_id,
                period_start,
                engagement_type,
                event_type,
                row_number() over (
                    partition by user_id, period_start, engagement_type
                    order by count(*) desc, event_type
                ) as rn
            from events_filtered
            where event_type is not null
            group by 1,2,3,4
        )
        where rn = 1

    ),

    primary_transaction_category as (

        select user_id, period_start, engagement_type, transaction_category as primary_transaction_category_in_period
        from (
            select
                user_id,
                period_start,
                engagement_type,
                transaction_category,
                row_number() over (
                    partition by user_id, period_start, engagement_type
                    order by count(*) desc, transaction_category
                ) as rn
            from events_filtered
            where transaction_category is not null
            group by 1,2,3,4
        )
        where rn = 1

    )

    select
        g.user_id,
        g.period_start,
        '{{ grain }}' as period_grain,
        g.engagement_type,
        g.growth_status,

        u.gender,
        u.country,

        fp.first_platform_in_period,
        fu.first_utm_source_in_period,
        fe.first_event_type_in_period,
        ft.first_transaction_category_in_period,

        pp.primary_platform_in_period,
        pu.primary_utm_source_in_period,
        pe.primary_event_type_in_period,
        pt.primary_transaction_category_in_period,

        coalesce(m.period_event_count, 0) as period_event_count,
        coalesce(m.period_miles_amount, 0) as period_miles_amount,
        coalesce(m.has_transaction_event, 0) as has_transaction_event,
        coalesce(m.has_pre_engagement_event, 0) as has_pre_engagement_event

    from growth_status g
    left join users u
      on g.user_id = u.user_id
    left join period_metrics m
      on g.user_id = m.user_id
     and g.period_start = m.period_start
     and g.engagement_type = m.engagement_type
    left join first_platform fp
      on g.user_id = fp.user_id
     and g.period_start = fp.period_start
     and g.engagement_type = fp.engagement_type
    left join first_utm fu
      on g.user_id = fu.user_id
     and g.period_start = fu.period_start
     and g.engagement_type = fu.engagement_type
    left join first_event_type fe
      on g.user_id = fe.user_id
     and g.period_start = fe.period_start
     and g.engagement_type = fe.engagement_type
    left join first_transaction_category ft
      on g.user_id = ft.user_id
     and g.period_start = ft.period_start
     and g.engagement_type = ft.engagement_type
    left join primary_platform pp
      on g.user_id = pp.user_id
     and g.period_start = pp.period_start
     and g.engagement_type = pp.engagement_type
    left join primary_utm pu
      on g.user_id = pu.user_id
     and g.period_start = pu.period_start
     and g.engagement_type = pu.engagement_type
    left join primary_event_type pe
      on g.user_id = pe.user_id
     and g.period_start = pe.period_start
     and g.engagement_type = pe.engagement_type
    left join primary_transaction_category pt
      on g.user_id = pt.user_id
     and g.period_start = pt.period_start
     and g.engagement_type = pt.engagement_type

{% endmacro %}