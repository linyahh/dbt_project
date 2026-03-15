with base as (

    select
        *
        
    from {{ ref('int_event_detail_activity') }}

)

,monthly as (select
    period_start_month as period_start,
    "monthly" as period_grain,
    user_flag_monthly as user_flag_of_period_grain,
    country,
    gender,
    transaction_category,
    platform,
    utm_source,
    count(distinct event_ts) as event_count,
    count(distinct user_id) as user_count,
    sum(CASE WHEN event_type="miles_earned" THEN coalesce(miles_amount, 0)ELSE 0 END) as total_miles_earned_amount,
    sum(CASE WHEN event_type="miles_redeemed" THEN coalesce(miles_amount, 0)ELSE 0 END) as total_miles_redeemed_amount

from base
group by all)

,weekly as (select
    period_start_week as period_start,
    "weekly" as period_grain,
    user_flag_weekly as user_flag_of_period_grain,
    country,
    gender,
    transaction_category,
    platform,
    utm_source,
    count(distinct event_ts) as event_count,
    count(distinct user_id) as user_count,
    sum(CASE WHEN event_type="miles_earned" THEN coalesce(miles_amount, 0)ELSE 0 END) as total_miles_earned_amount,
    sum(CASE WHEN event_type="miles_redeemed" THEN coalesce(miles_amount, 0)ELSE 0 END) as total_miles_redeemed_amount

from base
group by all)


,daily as (select
    event_date as period_start,
    "daily" as period_grain,
    user_flag_daily as user_flag_of_period_grain,
    country,
    gender,
    transaction_category,
    platform,
    utm_source,
    count(distinct event_ts) as event_count,
    count(distinct user_id) as user_count,
    sum(CASE WHEN event_type="miles_earned" THEN coalesce(miles_amount, 0)ELSE 0 END) as total_miles_earned_amount,
    sum(CASE WHEN event_type="miles_redeemed" THEN coalesce(miles_amount, 0)ELSE 0 END) as total_miles_redeemed_amount

from base
group by all)

SELECT * FROM daily
union all
SELECT * FROM weekly
union all
SELECT * FROM monthly