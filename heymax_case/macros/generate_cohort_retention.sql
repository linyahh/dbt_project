{% macro generate_cohort_retention(grain) %}

    {% if grain == 'week' %}
        {% set diff_unit = 'week' %}
        {% set cohort_col = 'cohort_week' %}
        {% set activity_col = 'activity_week' %}
    {% elif grain == 'month' %}
        {% set diff_unit = 'month' %}
        {% set cohort_col = 'cohort_month' %}
        {% set activity_col = 'activity_month' %}
    {% else %}
        {{ exceptions.raise_compiler_error("Invalid grain: " ~ grain) }}
    {% endif %}

with user_periods as (

    select
        user_id,
        {{ period_start_expr('event_date', grain) }} as period_start,
        engagement_bucket as engagement_type,
        any_value(country) as country,
        any_value(gender) as gender
    from {{ ref('fct_events') }}
    where engagement_bucket in ('pre_engagement', 'transaction')
    group by 1, 2, 3

    union all

    select
        user_id,
        {{ period_start_expr('event_date', grain) }} as period_start,
        'all' as engagement_type,
        any_value(country) as country,
        any_value(gender) as gender
    from {{ ref('fct_events') }}
    group by 1, 2, 3

),

user_first_activity as (

    select
        user_id,
        engagement_type,
        any_value(country) as country,
        any_value(gender) as gender,
        min(period_start) as {{ cohort_col }}
    from user_periods
    group by 1, 2

),

cohort_activity as (

    select
        f.user_id,
        f.engagement_type,
        f.country,
        f.gender,
        f.{{ cohort_col }},
        a.period_start as {{ activity_col }},
        date_diff(a.period_start, f.{{ cohort_col }}, {{ diff_unit }}) as period_number
    from user_first_activity f
    join user_periods a
      on f.user_id = a.user_id
     and f.engagement_type = a.engagement_type
    where a.period_start >= f.{{ cohort_col }}

),

cohort_sizes as (

    select
        {{ cohort_col }},
        engagement_type,
        country,
        gender,
        count(distinct user_id) as cohort_size
    from user_first_activity
    group by all

),

retention as (

    select
        {{ cohort_col }},
        engagement_type,
        country,
        gender,
        period_number,
        count(distinct user_id) as retained_users
    from cohort_activity
    group by all

)

select
    r.{{ cohort_col }} as cohort_period,
    r.period_number,
    c.engagement_type,
    c.country,
    c.gender,
    c.cohort_size,
    r.retained_users,
    safe_divide(r.retained_users, c.cohort_size) as retention_rate,
    '{{ grain }}' as period_grain
from retention r
join cohort_sizes c
  on r.{{ cohort_col }} = c.{{ cohort_col }}
 and r.engagement_type = c.engagement_type
order by
    cohort_period,
    period_number

{% endmacro %}
