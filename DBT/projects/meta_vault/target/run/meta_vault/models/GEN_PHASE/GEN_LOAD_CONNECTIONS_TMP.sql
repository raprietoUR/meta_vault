
  
    

        create or replace transient table edw.gen_phase.connections_tmp
         as
        (






-- depends on:edw.gen_phase.npsa_init


SELECT TMP.COD_CONNECTION,TMP.COD_AREA_PHASE,TMP.COD_TYPE_PHASE,TMP.DES_CONNECTION,TMP.COD_PATH_0,
TMP.COD_PATH_1,TMP.COD_PATH_2,TMP.COD_PATH_3
FROM  edw.raw_data.connections TMP
INNER JOIN  edw.gen_phase.model_load_runs_last M
 ON TMP.ID_MODEL_RUN=M.ID_MODEL_RUN
WHERE
  M.COD_TYPE_RUN='DEPLOY'
  AND M.COD_MODEL='EDW_TFG'
        );
      
  