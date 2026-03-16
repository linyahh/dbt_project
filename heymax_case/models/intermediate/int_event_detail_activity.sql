{{ config(tags=["refresh_daily"]) }}


with events as (

    select
        *
    from {{ ref('fct_events') }}

),

user_first_activity as (

    select
        user_id,
        min(event_date) as first_active_date,
        date_trunc(min(event_date), week) as first_active_week,
        date_trunc(min(event_date), month) as first_active_month
    from events
    group by 1

)

select
    e.*,
    e.event_date as period_start_day,
    date_trunc(e.event_date, week) as period_start_week,
    date_trunc(e.event_date, month) as period_start_month,

    case
        when e.event_date = ufa.first_active_date then 'new'
        else 'existing'
    end as user_flag_daily,

    case
        when date_trunc(e.event_date, week) = ufa.first_active_week then 'new'
        else 'existing'
    end as user_flag_weekly,

    case
        when date_trunc(e.event_date, month) = ufa.first_active_month then 'new'
        else 'existing'
    end as user_flag_monthly

from events e
left join user_first_activity ufa
    on e.user_id = ufa.user_id