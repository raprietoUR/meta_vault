

  
  
--depends on: edw.raw_data.datasets_fields_columns, edw.gen_phase.model_load_runs_last, edw.gen_phase.npsa_init

    SELECT
	TMP.TABLE_CATALOG,
	TMP.TABLE_SCHEMA,
	TMP.TABLE_NAME,
	TMP.COLUMN_NAME,
	TMP.ORDINAL_POSITION,
	TMP.COLUMN_DEFAULT,
	TMP.IS_NULLABLE,
	TMP.DATA_TYPE,
	TMP.CHARACTER_MAXIMUM_LENGTH,
	TMP.CHARACTER_OCTET_LENGTH,
	TMP.NUMERIC_PRECISION,
	TMP.NUMERIC_PRECISION_RADIX,
	TMP.NUMERIC_SCALE,
	TMP.DATETIME_PRECISION,
	TMP.INTERVAL_TYPE,
	TMP.INTERVAL_PRECISION,
	TMP.CHARACTER_SET_CATALOG,
	TMP.CHARACTER_SET_SCHEMA,
	TMP.CHARACTER_SET_NAME,
	TMP.COLLATION_CATALOG,
	TMP.COLLATION_SCHEMA,
	TMP.COLLATION_NAME,
	TMP.DOMAIN_CATALOG,
	TMP.DOMAIN_SCHEMA,
	TMP.DOMAIN_NAME,
	TMP.UDT_CATALOG,
	TMP.UDT_SCHEMA,
	TMP.UDT_NAME,
	TMP.SCOPE_CATALOG,
	TMP.SCOPE_SCHEMA,
	TMP.SCOPE_NAME,
	TMP.MAXIMUM_CARDINALITY,
	TMP.DTD_IDENTIFIER,
	TMP.IS_SELF_REFERENCING,
	TMP.IS_IDENTITY,
	TMP.IDENTITY_GENERATION,
	TMP.IDENTITY_START,
	TMP.IDENTITY_INCREMENT,
	TMP.IDENTITY_MAXIMUM,
	TMP.IDENTITY_MINIMUM,
	TMP.IDENTITY_CYCLE,
	TMP.COMMENT
FROM  edw.raw_data.datasets_fields_columns TMP
INNER JOIN  edw.gen_phase.model_load_runs_last M
 ON TMP.ID_MODEL_RUN=M.ID_MODEL_RUN
WHERE
M.COD_TYPE_RUN='COD_TYPE_RUN'
AND M.COD_MODEL= 'COD_MODEL'