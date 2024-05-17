






    SELECT 	
    'DEPLOY'||'-'||to_char(CURRENT_TIMESTAMP::TIMESTAMP, 'yyyymmddhh24missFF3') AS COD_RUN,
    'EDW_TFG' as COD_MODEL,
    to_varchar(CURRENT_TIMESTAMP::TIMESTAMPNTZ, 'yyyymmddhh24missFF3')  as DT_MODEL_LOAD,
    'DEPLOY' AS COD_TYPE_RUN,
    'EDW_TFG'||'-'||'DEPLOY'||'-'||to_char(CURRENT_TIMESTAMP::TIMESTAMP, 'yyyymmddhh24missFF3') AS DES_RUNS,
    MD5('DEPLOY'||'-'||to_char(CURRENT_TIMESTAMP::TIMESTAMP, 'yyyymmddhh24missFF3')||'-'||'EDW_TFG') AS ID_MODEL_RUN