{{ config(
    materialized='incremental',
    unique_key='event_ts',
    incremental_strategy='merge',
    partition_by={
        "field": "event_ts",
        "data_type": "timestamp"
    },
    cluster_by=["user_id"]
) }}

with source_events as (

    select
        user_id,
        event_ts,
        date(event_ts) as event_date,
        event_type,
        transaction_category,
        miles_amount,
        platform,
        utm_source,
        country,
        gender
    from {{ ref('stg_event_stream') }}

),

filtered as (

    select *
    from source_events
    where event_date >= cast('{{ var("backfill_start_date") }}' as date)

    {% if is_incremental() %}

        and event_date >= (
            select coalesce(max(event_date), date('1970-01-01'))
            from {{ this }}
        )

    {% endif %}

)

select *
from filtered