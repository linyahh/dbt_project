{{ config(materialized='view') }}


SELECT * FROM {{ ref('fct_cohort_retention_monthly') }}
UNION ALL 
SELECT * FROM {{ ref('fct_cohort_retention_weekly') }}
