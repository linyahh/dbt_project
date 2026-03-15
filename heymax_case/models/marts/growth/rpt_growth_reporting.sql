{{ config(materialized='table') }}

{{ generate_growth_reporting_aggregation(
    source_model='fct_growth_user_features_combined',

    dimensions=[
        {'name': 'period_start', 'expr': 'period_start', 'type': 'date'},
        {'name': 'period_grain', 'expr': 'period_grain', 'type': 'string'},
        {'name': 'engagement_type', 'expr': 'engagement_type', 'type': 'string'},
        {'name': 'growth_status', 'expr': 'growth_status', 'type': 'string'},
        {'name': 'country', 'expr': 'country', 'type': 'string'},
        {'name': 'gender', 'expr': 'gender', 'type': 'string'},
        {'name': 'event_type', 'expr': 'primary_event_type_in_period', 'type': 'string'},
        {'name': 'utm_source', 'expr': 'primary_utm_source_in_period', 'type': 'string'},
        {'name': 'platform', 'expr': 'primary_platform_in_period', 'type': 'string'},
        {'name': 'transaction_category', 'expr': 'primary_transaction_category_in_period', 'type': 'string'}
    ],

    default_dimensions=['period_start', 'period_grain','engagement_type', 'growth_status'],

    aggregation_sets=[
        ['country'],
        ['country','gender'],
        ['country','event_type'],
        ['country','platform'],
        ['country','event_type','utm_source'],
        ['country','platform', 'event_type','utm_source'],
        ['country','platform','transaction_category' ,'event_type','utm_source'],
        
    ],

    metrics=[
        {'name': 'users', 'expr': 'count(distinct user_id)'},
        {'name': 'total_events', 'expr': 'sum(period_event_count)'},
        {'name': 'total_miles_amount', 'expr': 'sum(period_miles_amount)'},
        {'name': 'avg_events_per_user', 'expr': 'avg(period_event_count)'},
        {'name': 'avg_miles_per_user', 'expr': 'avg(period_miles_amount)'}
    ],

    include_overall=true
) }}

