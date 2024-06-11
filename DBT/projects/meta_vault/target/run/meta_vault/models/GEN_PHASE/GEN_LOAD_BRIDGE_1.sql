
  
    

        create or replace transient table edw.gen_phase.bridge_1
         as
        (



SELECT BR.COD_SETCONNECTION
,BR.COD_DATASET
,BR.COD_DATASET_NAME
,BR.COD_TYPE_DATASET
,BR.COD_MODEL
,BR.SW_AUTOMATED_DV
,BR.COD_TYPE_LOAD_SOURCE
,BR.COD_DATASET_ORIGIN
,BR.COD_TYPE_ENTITY
,BR.ID_ORDER
,BR.COD_DATASET_FIELD
, CASE WHEN BR.COD_TYPE_DATASET = 'HUB' THEN 'PK' ELSE 'FK' END COD_TYPE
, CASE WHEN BR.COD_TYPE_DATASET = 'SAT' THEN 'HUB_ID_'||BR_2.COD_DATASET_NAME WHEN BR.COD_TYPE_DATASET = 'HUB' THEN 'HUB_ID_'||BR.COD_DATASET_NAME  ELSE 'LINK_ID_'||BR.COD_DATASET_NAME  END  COD_COLUMN_NAME_TARGET
,BR.SW_TRACK_DIFF
,BR.COLUMN_VALUE_DEFAULT_FX
,BR.SW_VIRTUAL_FIELD_VALUE
,BR.DT_MODEL_LOAD
,BR.ID_MODEL_RUN
FROM edw.gen_phase.bridge_0 BR LEFT JOIN
    edw.gen_phase.datasets_segments DSEG ON BR.COD_DATASET = DSEG.COD_DATASET_CHILD LEFT JOIN
    edw.gen_phase.bridge_0 BR_2 ON DSEG.COD_DATASET = BR_2.COD_DATASET
WHERE BR.COD_TYPE = 'BK'
        );
      
  