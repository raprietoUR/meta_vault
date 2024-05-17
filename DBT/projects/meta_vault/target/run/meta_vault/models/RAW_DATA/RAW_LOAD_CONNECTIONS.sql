
  
    

        create or replace transient table edw.raw_data.connections
         as
        (
-- depends_on: edw.raw_data.raw_init
SELECT 		T.*,
			M.ID_MODEL_RUN,
			M.DT_MODEL_LOAD,
			CURRENT_TIMESTAMP DT_LOAD
	FROM  EDW.raw_data.connections_tmp T
LEFT JOIN (SELECT
			COD_MODEL,
			DT_MODEL_LOAD,
			ID_MODEL_RUN,
			ROW_NUMBER() OVER (PARTITION BY COD_MODEL ORDER BY DT_MODEL_LOAD DESC) ULTIMO_REG
		FROM  edw.raw_data.model_load_runs WHERE COD_TYPE_RUN='DEPLOY') M
ON M.COD_MODEL = 'EDW_TFG'
AND M.ULTIMO_REG = 1
        );
      
  