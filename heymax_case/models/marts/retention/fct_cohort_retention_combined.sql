{{ config(materialized='view',tags=["refresh_daily"] ) }}


SELECT * FROM {{ ref('fct_cohort_retention_monthly') }}
UNION ALL 
SELECT * FROM {{ ref('fct_cohort_retention_weekly') }}
