{{ config(
    schema='gen_phase',
    alias='datasets_0',
    materialized='incremental'
) }}

SELECT COD_SETCONNECTION
	, COD_DATASET	
	, COD_TYPE	
	, COD_MODEL	
	, COD_TYPE_LOAD_SOURCE	
	, COD_DATASET_ORIGIN	
	, "DESC"
	, ID_MODEL_RUN	
	, DT_MODEL_LOAD	
	, DT_LOAD
	, 0 SW_IS_SAT_MAS
	,'HIST'::VARCHAR(100) COD_FRESHNESS
	, 1 SW_INCREMENTAL
	, 0 SW_RECREATE
FROM {{ ref('RAW_LOAD_DATASETS') }} 
