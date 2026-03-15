{{ config(materialized='view') }}


SELECT * FROM {{ ref('fct_growth_user_features_daily') }}
UNION ALL 
SELECT * FROM {{ ref('fct_growth_user_features_weekly') }}
UNION ALL 
SELECT * FROM {{ ref('fct_growth_user_features_monthly') }}
