

SELECT SEG.COD_DATASET_CHILD COD_DATASET
, CAST(DS_FIELDS.ID_ORDER AS VARCHAR(2)) ID_ORDER
, DS_FIELDS.COD_DATASET_FIELD
, DS_FIELDS.COD_TYPE
, DS_FIELDS.COD_COLUMN_NAME_TARGET
, '0' SW_TRACK_DIFF /* BK, PK, SQ a 0*/
, DS_FIELDS.COLUMN_VALUE_DEFAULT_FX
, 0 SW_VIRTUAL_FIELD_VALUE
, DS_FIELDS.DESC_BUSINESS_DESCRIPTION
FROM  edw.gen_phase.datasets_fields_0 DS_FIELDS
INNER JOIN  edw.gen_phase.datasets_segments SEG  ON DS_FIELDS.COD_DATASET = SEG.COD_DATASET
WHERE DS_FIELDS.COD_TYPE IN ('BK','PK','SQ')
UNION ALL
SELECT * FROM  edw.gen_phase.datasets_fields_0