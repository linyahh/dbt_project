{{ config(materialized='incremental', tags=["refresh_monthly"]) }}

{{ generate_cohort_retention('month') }}