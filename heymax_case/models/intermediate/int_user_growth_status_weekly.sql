{%- set refresh_tag = (
    "refresh_daily" if "daily" in this.name else
    "refresh_weekly" if "weekly" in this.name else
    "refresh_monthly" if "monthly" in this.name else
    none
) -%}

{%- if refresh_tag -%}
{{ config(tags=[refresh_tag]) }}
{%- endif -%}

{{ generate_user_growth_status('week') }}