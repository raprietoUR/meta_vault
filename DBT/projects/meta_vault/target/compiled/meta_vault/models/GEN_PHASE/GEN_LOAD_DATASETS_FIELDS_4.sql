

SELECT COD_DATASET ,
    ID_ORDER,
    COD_DATASET_FIELD,
    'BK' COD_TYPE,
    COD_COLUMN_NAME_TARGET,
    SW_TRACK_DIFF,
    COLUMN_VALUE_DEFAULT_FX,  
    SW_VIRTUAL_FIELD_VALUE,  
    DESC_BUSINESS_DESCRIPTION
FROM edw.gen_phase.datasets_fields_2