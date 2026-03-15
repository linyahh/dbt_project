{{ config(materialized='table') }}

with base as (

    select * from {{ ref('int_user_features_daily') }}
    union all
    select * from {{ ref('int_user_features_weekly') }}
    union all
    select * from {{ ref('int_user_features_monthly') }}

)

select
    * except (user_id,event_count,miles_earned,miles_redeemed),

    count(distinct user_id) as users,
    sum(event_count) as event_count,
    sum(miles_earned) as miles_earned,
    sum(miles_redeemed) as miles_redeemed

from base
group by all
