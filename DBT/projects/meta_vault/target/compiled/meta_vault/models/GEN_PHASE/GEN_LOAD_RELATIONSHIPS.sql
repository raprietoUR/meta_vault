

-- depends on: edw.gen_phase.npsa_init


SELECT TMP.COD_RELATIONSHIP,TMP.COD_RELATIONSHIP_NAME
,TMP.REFERENCE_COD_DATASET,TMP.REFERENCE_COD_NAME ,TMP.TARGET_COD_DATASET,TMP.TARGET_COD_NAME,TMP.TARGET_FINAL_COD_NAME, TMP.DESC_BUSINESS_DEFINITION
FROM  edw.raw_data.relationships TMP
INNER JOIN  edw.gen_phase.model_load_runs_last M
 ON TMP.ID_MODEL_RUN=M.ID_MODEL_RUN
