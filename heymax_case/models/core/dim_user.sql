{{ config(
    materialized='incremental',
    unique_key='user_id',
    incremental_strategy='merge',
    partition_by={
        "field": "first_seen_date",
        "data_type": "date"
    },
    cluster_by=["user_id"]
) }}

with events as (

    select *
    from {{ ref('stg_event_stream') }}

),

user_stats as (

    select
        user_id,

        any_value(gender) as gender,
        any_value(country) as country,
        --We should derive one gender per user,Or take the first recorded gender.

        min(event_ts) as first_seen_ts,
        max(event_ts) as last_seen_ts,

        min(event_date) as first_seen_date,
        max(event_date) as last_seen_date,

        count(distinct event_ts) as lifetime_events

    from events
    group by user_id

)

select *
from user_stats