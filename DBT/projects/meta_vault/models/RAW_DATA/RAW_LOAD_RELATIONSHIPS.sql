{{ config(
  alias='relationships',
  materialized='incremental') }}
-- depends_on: {{ ref('RAW_INIT') }}
SELECT 		
	T.*,
	M.ID_MODEL_RUN,
	M.DT_MODEL_LOAD,
	CURRENT_TIMESTAMP DT_LOAD
	FROM  {{ source('EDW_RAW_DATA', 'relationships_tmp') }} T
LEFT JOIN (SELECT
			COD_MODEL,
			DT_MODEL_LOAD,
			ID_MODEL_RUN,
			ROW_NUMBER() OVER (PARTITION BY COD_MODEL ORDER BY DT_MODEL_LOAD DESC) ULTIMO_REG
		FROM  {{ ref('RAW_LOAD_MODEL_LOAD_RUNS') }} WHERE COD_TYPE_RUN='{{ var('cod_type_run') }}') M
ON M.COD_MODEL = '{{ var('cod_model') }}'
AND M.ULTIMO_REG = 1