with source as (

    select *
    from {{ source('heymax', 'event_stream_raw') }}

),

cleaned as (

    select
        cast(event_time as timestamp) as event_ts,
        date(event_time) as event_date,

        user_id,
        gender,
        event_type,
        transaction_category,

        cast(miles_amount as numeric) as miles_amount,

        platform,
        utm_source,
        country

    from source

)

select * from cleaned