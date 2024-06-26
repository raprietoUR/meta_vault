{{ config(
    schema='gen_phase',
    alias='datasets_fields_1',
    materialized='incremental'
) }}

SELECT SEG.COD_DATASET_CHILD COD_DATASET
, CAST(DS_FIELDS.ID_ORDER AS VARCHAR(2)) ID_ORDER
, DS_FIELDS.COD_DATASET_FIELD
, DS_FIELDS.COD_TYPE
, DS_FIELDS.COD_COLUMN_NAME_TARGET
, '0' SW_TRACK_DIFF /* BK, PK, SQ a 0*/
, DS_FIELDS.COLUMN_VALUE_DEFAULT_FX
, 0 SW_VIRTUAL_FIELD_VALUE
, DS_FIELDS.DESC_BUSINESS_DESCRIPTION
FROM  {{ ref('GEN_LOAD_DATASETS_FIELDS_0') }} DS_FIELDS
INNER JOIN  {{ ref('GEN_LOAD_DATASETS_SEGMENTS') }} SEG  ON DS_FIELDS.COD_DATASET = SEG.COD_DATASET
WHERE DS_FIELDS.COD_TYPE IN ('BK','PK','SQ')
UNION ALL
SELECT * FROM  {{ ref('GEN_LOAD_DATASETS_FIELDS_0') }}