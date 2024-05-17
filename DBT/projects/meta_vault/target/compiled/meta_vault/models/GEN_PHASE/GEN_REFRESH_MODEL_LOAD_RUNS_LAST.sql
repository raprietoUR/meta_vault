

-- depends_on: edw.gen_phase.npsa_init







SELECT * from
(select COD_RUN,
        COD_MODEL,
        COD_TYPE_RUN,
        DES_RUNS,
  
        to_timestamp(DT_MODEL_LOAD,'yyyymmddhh24missFF3') DT_MODEL_LOAD,
  
        DT_MODEL_LOAD DT_START,
        coalesce(LAG(DT_MODEL_LOAD,1) OVER (PARTITION BY COD_MODEL,COD_TYPE_RUN ORDER BY DT_MODEL_LOAD DESC), '01/01/2020') DT_END,
        ROW_NUMBER() OVER (PARTITION BY COD_MODEL,COD_TYPE_RUN ORDER BY DT_MODEL_LOAD DESC) ID_ORDER,
        ID_MODEL_RUN
    FROM  edw.raw_data.model_load_runs 
    WHERE COD_MODEL = 'EDW_TFG'
    AND COD_TYPE_RUN = 'DEPLOY'
    ) MLR
    where ID_ORDER = 1