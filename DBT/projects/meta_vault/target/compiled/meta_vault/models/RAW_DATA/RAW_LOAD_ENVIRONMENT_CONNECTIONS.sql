
-- depends_on: edw.RAW_DATA.raw_init
SELECT 		T.*,
			M.ID_MODEL_RUN,
			M.DT_MODEL_LOAD,
			CURRENT_TIMESTAMP DT_LOAD
	FROM  EDW.raw_data.environment_connections_tmp T
LEFT JOIN (SELECT
			COD_MODEL,
			DT_MODEL_LOAD,
			ID_MODEL_RUN,
			ROW_NUMBER() OVER (PARTITION BY COD_MODEL ORDER BY DT_MODEL_LOAD DESC) ULTIMO_REG
		FROM  edw.RAW_DATA.model_load_runs WHERE COD_TYPE_RUN='DEPLOY') M
ON M.COD_MODEL = 'DV_TFG'
AND M.ULTIMO_REG = 1