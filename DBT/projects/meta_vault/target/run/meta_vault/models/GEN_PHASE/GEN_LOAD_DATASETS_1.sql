
  
    

        create or replace transient table edw.gen_phase.datasets_1
         as
        (

SELECT TMP.COD_DATASET,TMP.ID_ORDER,TMP.COD_DATASET_FIELD,TMP.COD_TYPE,TMP.COD_COLUMN_NAME_TARGET, TMP."DESC"
FROM  edw.raw_data.datasets TMP
INNER JOIN  edw.gen_phase.model_load_runs_last M
 ON TMP.ID_MODEL_RUN=M.ID_MODEL_RUN
WHERE
M.COD_TYPE_RUN='DEPLOY'
AND M.COD_MODEL= 'DV_TFG'
        );
      
  