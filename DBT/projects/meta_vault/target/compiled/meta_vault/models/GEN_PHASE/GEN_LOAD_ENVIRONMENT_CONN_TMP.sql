

  
  

-- depends on: edw.gen_phase.npsa_init


SELECT TMP.COD_ENVIRONMENT,TMP.COD_CONNECTION,TMP.DESC_ENVIRONMENT_CONNECTION
FROM  edw.raw_data.environment_connections TMP
INNER JOIN  edw.gen_phase.model_load_runs_last M
 ON TMP.ID_MODEL_RUN=M.ID_MODEL_RUN
WHERE
M.COD_TYPE_RUN='DEPLOY'
AND M.COD_MODEL= 'EDW_TFG'