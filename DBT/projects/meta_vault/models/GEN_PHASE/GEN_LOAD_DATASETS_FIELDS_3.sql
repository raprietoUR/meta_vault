{{ config(
    schema='gen_phase',
    alias='datasets_fields_3',
    materialized='incremental'
) }}

SELECT * 
FROM  {{ ref('GEN_LOAD_DATASETS_FIELDS_1') }} 
UNION ALL
SELECT * 
FROM  {{ ref('GEN_LOAD_DATASETS_FIELDS_2') }}