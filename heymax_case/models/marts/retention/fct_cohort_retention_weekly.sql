{{ config(materialized='incremental', tags=["refresh_weekly"]) }}

{{ generate_cohort_retention('week') }}