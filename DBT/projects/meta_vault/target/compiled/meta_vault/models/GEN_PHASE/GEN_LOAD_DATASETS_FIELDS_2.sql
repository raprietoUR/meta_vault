

SELECT COD_DATASET ,
    CAST(ROW_NUMBER () OVER (PARTITION BY COD_DATASET ORDER BY COD_DATASET_FIELD ASC) AS VARCHAR(2)) ID_ORDER,
    COD_DATASET_FIELD,
    COD_TYPE,
    COD_COLUMN_NAME_TARGET,
    SW_TRACK_DIFF,
    COLUMN_VALUE_DEFAULT_FX,  
    SW_VIRTUAL_FIELD_VALUE,  
    DESC_BUSINESS_DESCRIPTION 
FROM (SELECT DISTINCT REL.COD_RELATIONSHIP COD_DATASET ,
    CAST(DS_FIELDS.ID_ORDER AS VARCHAR(2)) ID_ORDER,
    DS_FIELDS.COD_DATASET_FIELD,
    'FK' COD_TYPE,
    DS_FIELDS.COD_COLUMN_NAME_TARGET,
    '0' SW_TRACK_DIFF, --PK y FK a 0 
    DS_FIELDS.COLUMN_VALUE_DEFAULT_FX,  
    DS_FIELDS.SW_VIRTUAL_FIELD_VALUE,  
    DS_FIELDS.DESC_BUSINESS_DESCRIPTION 
FROM  edw.gen_phase.relationships_final REL JOIN
	 edw.gen_phase.datasets_fields_1 DS_FIELDS ON REL.REFERENCE_COD_DATASET = DS_FIELDS.COD_DATASET 
UNION	 
SELECT DISTINCT REL.COD_RELATIONSHIP COD_DATASET ,
    CAST(DS_FIELDS.ID_ORDER AS VARCHAR(2)) ID_ORDER,
    DS_FIELDS.COD_DATASET_FIELD,
    'FK' COD_TYPE,
    DS_FIELDS.COD_COLUMN_NAME_TARGET,
    '0' SW_TRACK_DIFF, --PK y FK a 0 
    DS_FIELDS.COLUMN_VALUE_DEFAULT_FX,  
    DS_FIELDS.SW_VIRTUAL_FIELD_VALUE,  
    DS_FIELDS.DESC_BUSINESS_DESCRIPTION 
FROM  edw.gen_phase.relationships_final REL JOIN
	 edw.gen_phase.datasets_fields_1 DS_FIELDS ON REL.TARGET_COD_DATASET = DS_FIELDS.COD_DATASET )