{{ config(
    schema='gen_phase',
    alias='datasets',
    materialized='incremental'
) }}

SELECT * 
FROM  {{ ref('GEN_LOAD_DATASETS_0') }}
	UNION ALL
	SELECT * 
  FROM  {{ ref('GEN_LOAD_DATASETS_1') }}