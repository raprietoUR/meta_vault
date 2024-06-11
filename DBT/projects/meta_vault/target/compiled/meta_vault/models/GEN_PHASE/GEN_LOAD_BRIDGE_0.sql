



SELECT DS.COD_SETCONNECTION
, DS.COD_DATASET
--, DS.COD_DATASET_NAME
, DS.COD_TYPE COD_TYPE_DATASET
, DS.COD_MODEL 
, DS.SW_AUTOMATED_DV 
, DS.COD_TYPE_LOAD_SOURCE 
, DS.COD_DATASET_ORIGIN 
, DS.COD_TYPE_ENTITY
, DS_FIELDS.ID_ORDER 
, DS_FIELDS.COD_DATASET_FIELD 
, DS_FIELDS.COD_TYPE 
, DS_FIELDS.COD_COLUMN_NAME_TARGET 
, DS_FIELDS.SW_TRACK_DIFF 
, DS_FIELDS.COLUMN_VALUE_DEFAULT_FX 
, DS_FIELDS.SW_VIRTUAL_FIELD_VALUE 
, DS.DT_MODEL_LOAD 
, DS.ID_MODEL_RUN 
FROM edw.gen_phase.datasets DS
	LEFT JOIN edw.gen_phase.datasets_fields DS_FIELDS ON DS.COD_DATASET = DS_FIELDS.COD_DATASET