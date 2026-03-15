{% macro engagement_type_case(event_type_col) %}

    case
        when {{ event_type_col }} in ('like', 'share', 'reward_search') then 'pre_engagement'
        when {{ event_type_col }} in ('miles_earned', 'miles_redeemed') then 'transaction'
        else 'other'
    end

{% endmacro %}