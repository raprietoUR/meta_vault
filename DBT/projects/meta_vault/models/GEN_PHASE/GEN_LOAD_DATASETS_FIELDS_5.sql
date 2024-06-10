{{ config(
    schema='gen_phase',
    alias='datasets_fields',
    materialized='incremental'
) }}

SELECT DISTINCT * 
FROM  {{ ref('GEN_LOAD_DATASETS_FIELDS_3') }} 
UNION ALL
SELECT DISTINCT * 
FROM  {{ ref('GEN_LOAD_DATASETS_FIELDS_4') }}