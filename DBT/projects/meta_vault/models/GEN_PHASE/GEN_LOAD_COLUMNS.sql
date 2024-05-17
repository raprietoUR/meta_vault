{{ config(schema='gen_phase',
  alias='datasets_fields_columns',
  materialized='incremental') }}

  
  
--depends on: {{ ref('RAW_LOAD_COLUMNS') }}, {{ ref('GEN_REFRESH_MODEL_LOAD_RUNS_LAST') }}, {{ ref('GEN_INIT') }}

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
FROM  {{ ref('RAW_LOAD_COLUMNS') }} TMP
INNER JOIN  {{ ref('GEN_REFRESH_MODEL_LOAD_RUNS_LAST') }} M
 ON TMP.ID_MODEL_RUN=M.ID_MODEL_RUN
WHERE
M.COD_TYPE_RUN='{{ var('cod_type_run') }}'
AND M.COD_MODEL= '{{ var('cod_model') }}'

