DROP TABLE IF EXISTS RAW_DATA.ENVIRONMENTS_TMP;
DROP TABLE IF EXISTS RAW_DATA.CONNECTIONS_TMP;
DROP TABLE IF EXISTS RAW_DATA.ENVIRONMENT_CONNECTIONS_TMP;
DROP TABLE IF EXISTS RAW_DATA.SETCONNECTION_CONNECTIONS_TMP;
DROP TABLE IF EXISTS RAW_DATA.MODEL_TMP;
DROP TABLE IF EXISTS RAW_DATA.DATASETS_TMP;
DROP TABLE IF EXISTS RAW_DATA.DATASETS_SEGMENTS_TMP;
DROP TABLE IF EXISTS RAW_DATA.RELATIONSHIPS_TMP;
DROP TABLE IF EXISTS RAW_DATA.DATASETS_FIELDS_TMP;
DROP TABLE IF EXISTS  RAW_DATA.DATASETS_FIELDS_COLUMNS_TMP;



create table if not exists RAW_DATA.DATASETS_FIELDS_COLUMNS_TMP (
	TABLE_CATALOG				VARCHAR(10485760),
	TABLE_SCHEMA				VARCHAR(10485760),
	TABLE_NAME					VARCHAR(10485760),
	COLUMN_NAME					VARCHAR(10485760),
	ORDINAL_POSITION			VARCHAR(10485760),
	COLUMN_DEFAULT				VARCHAR(10485760),
	IS_NULLABLE					VARCHAR(10485760),
	DATA_TYPE					VARCHAR(10485760),
	CHARACTER_MAXIMUM_LENGTH	VARCHAR(10485760),
	CHARACTER_OCTET_LENGTH		VARCHAR(10485760),
	NUMERIC_PRECISION			VARCHAR(10485760),
	NUMERIC_PRECISION_RADIX		VARCHAR(10485760),
	NUMERIC_SCALE				VARCHAR(10485760),
	DATETIME_PRECISION			VARCHAR(10485760),
	INTERVAL_TYPE				VARCHAR(10485760),
	INTERVAL_PRECISION			VARCHAR(10485760),
	CHARACTER_SET_CATALOG		VARCHAR(10485760),
	CHARACTER_SET_SCHEMA		VARCHAR(10485760),
	CHARACTER_SET_NAME			VARCHAR(10485760),
	COLLATION_CATALOG			VARCHAR(10485760),
	COLLATION_SCHEMA			VARCHAR(10485760),
	COLLATION_NAME				VARCHAR(10485760),
	DOMAIN_CATALOG				VARCHAR(10485760),
	DOMAIN_SCHEMA				VARCHAR(10485760),
	DOMAIN_NAME					VARCHAR(10485760),
	UDT_CATALOG					VARCHAR(10485760),
	UDT_SCHEMA					VARCHAR(10485760),
	UDT_NAME					VARCHAR(10485760),
	SCOPE_CATALOG				VARCHAR(10485760),
	SCOPE_SCHEMA				VARCHAR(10485760),
	SCOPE_NAME					VARCHAR(10485760),
	MAXIMUM_CARDINALITY			VARCHAR(10485760),
	DTD_IDENTIFIER				VARCHAR(10485760),
	IS_SELF_REFERENCING			VARCHAR(10485760),
	IS_IDENTITY					VARCHAR(10485760),
	IDENTITY_GENERATION			VARCHAR(10485760),
	IDENTITY_START				VARCHAR(10485760),
	IDENTITY_INCREMENT			VARCHAR(10485760),
	IDENTITY_MAXIMUM			VARCHAR(10485760),
	IDENTITY_MINIMUM			VARCHAR(10485760),
	IDENTITY_CYCLE				VARCHAR(10485760),
	COMMENT						VARCHAR(10485760)
);

create table if not exists RAW_DATA.ENVIRONMENTS_TMP (
	COD_ENVIRONMENT				VARCHAR(10485760),
	DESC_ENVIRONMENT				VARCHAR(10485760),
    WAREHOUSE_NAME				VARCHAR(10485760)
);

create table if not exists RAW_DATA.CONNECTIONS_TMP (
	COD_CONNECTION				VARCHAR(10485760),
    COD_AREA_PHASE				VARCHAR(10485760),
    COD_TYPE_PHASE				VARCHAR(10485760),
    DES_CONNECTION				VARCHAR(10485760),
    COD_PATH_0				VARCHAR(10485760),
    COD_PATH_1				VARCHAR(10485760),
    COD_PATH_2				VARCHAR(10485760),
    COD_PATH_3				VARCHAR(10485760)
);


create table if not exists RAW_DATA.ENVIRONMENT_CONNECTIONS_TMP (
	COD_ENVIRONMENT				VARCHAR(10485760),
	COD_CONNECTION         VARCHAR(10485760),
    DESC_ENVIRONMENT_CONNECTION				VARCHAR(10485760)
);


create table if not exists RAW_DATA.SETCONNECTION_CONNECTIONS_TMP (
	COD_SETCONNECTION				VARCHAR(10485760),
    COD_CONNECTION				VARCHAR(10485760),
    DES_SETCONNECTION_CONNECTION				VARCHAR(10485760)
);


create table if not exists RAW_DATA.MODEL_TMP (
	COD_MODEL				VARCHAR(10485760),
	DESC_MODEL				VARCHAR(10485760),
    COD_PROJECT             VARCHAR(10485760)
);

create table if not exists RAW_DATA.DATASETS_TMP (
	COD_SETCONNECTION				VARCHAR(10485760),
	COD_DATASET				VARCHAR(10485760),
    COD_TYPE                VARCHAR(10485760),
    COD_MODEL               VARCHAR(10485760),
    COD_TYPE_LOAD_SOURCE    VARCHAR(10485760),
    COD_DATASET_ORIGIN      VARCHAR(10485760),
	COD_DATASET_NAME		VARCHAR(10485760),
	COD_DATASET_BUSINESS	VARCHAR(10485760),
    DESC_BUSINESS_DEFINITION	VARCHAR(10485760)
);

create table if not exists RAW_DATA.DATASETS_SEGMENTS_TMP (
	COD_DATASET				VARCHAR(10485760),
	ID				VARCHAR(10485760),
    COD_DATASET_CHILD       VARCHAR(10485760),
    COD_DATASET_NAME        VARCHAR(10485760),
    DESC_BUSINESS_DEFINITION                    VARCHAR(10485760)
);

create table if not exists RAW_DATA.RELATIONSHIPS_TMP (
	COD_RELATIONSHIP				VARCHAR(10485760),
	COD_RELATIONSHIP_NAME				VARCHAR(10485760),
    REFERENCE_COD_DATASET           VARCHAR(10485760),
    REFERENCE_COD_NAME              VARCHAR(10485760),
    TARGET_COD_DATASET              VARCHAR(10485760),
    TARGET_COD_NAME                 VARCHAR(10485760),
    TARGET_FINAL_COD_NAME           VARCHAR(10485760),
    DESC_BUSINESS_DEFINITION                            VARCHAR(10485760)
);

create table if not exists RAW_DATA.DATASETS_FIELDS_TMP (
	COD_DATASET				VARCHAR(10485760),
	ID_ORDER				VARCHAR(10485760),
    COD_DATASET_FIELD       VARCHAR(10485760),
    COD_TYPE                VARCHAR(10485760),
    COD_COLUMN_NAME_TARGET  VARCHAR(10485760),
    DESC_BUSINESS_DEFINITION                    VARCHAR(10485760)
);





DROP TABLE IF EXISTS RAW_DATA.ENVIRONMENTS;
DROP TABLE IF EXISTS RAW_DATA.ENVIRONMENT_CONNECTIONS;
DROP TABLE IF EXISTS RAW_DATA.MODEL;
DROP TABLE IF EXISTS RAW_DATA.DATASETS;
DROP TABLE IF EXISTS RAW_DATA.DATASETS_SEGMENTS;
DROP TABLE IF EXISTS RAW_DATA.RELATIONSHIPS;
DROP TABLE IF EXISTS RAW_DATA.DATASETS_FIELDS;
DROP TABLE IF EXISTS  RAW_DATA.DATASETS_FIELDS_COLUMNS;



create table if not exists RAW_DATA.DATASETS_FIELDS_COLUMNS (
	TABLE_CATALOG				VARCHAR(10485760),
	TABLE_SCHEMA				VARCHAR(10485760),
	TABLE_NAME					VARCHAR(10485760),
	COLUMN_NAME					VARCHAR(10485760),
	ORDINAL_POSITION			NUMBER(9,0),
	COLUMN_DEFAULT				VARCHAR(10485760),
	IS_NULLABLE					VARCHAR(10485760),
	DATA_TYPE					VARCHAR(10485760),
	CHARACTER_MAXIMUM_LENGTH	NUMBER(9,0),
	CHARACTER_OCTET_LENGTH		NUMBER(9,0),
	NUMERIC_PRECISION			NUMBER(9,0),
	NUMERIC_PRECISION_RADIX		NUMBER(9,0),
	NUMERIC_SCALE				NUMBER(9,0),
	DATETIME_PRECISION			NUMBER(9,0),
	INTERVAL_TYPE				VARCHAR(10485760),
	INTERVAL_PRECISION			NUMBER(9,0),
	CHARACTER_SET_CATALOG		VARCHAR(10485760),
	CHARACTER_SET_SCHEMA		VARCHAR(10485760),
	CHARACTER_SET_NAME			VARCHAR(10485760),
	COLLATION_CATALOG			VARCHAR(10485760),
	COLLATION_SCHEMA			VARCHAR(10485760),
	COLLATION_NAME				VARCHAR(10485760),
	DOMAIN_CATALOG				VARCHAR(10485760),
	DOMAIN_SCHEMA				VARCHAR(10485760),
	DOMAIN_NAME					VARCHAR(10485760),
	UDT_CATALOG					VARCHAR(10485760),
	UDT_SCHEMA					VARCHAR(10485760),
	UDT_NAME					VARCHAR(10485760),
	SCOPE_CATALOG				VARCHAR(10485760),
	SCOPE_SCHEMA				VARCHAR(10485760),
	SCOPE_NAME					VARCHAR(10485760),
	MAXIMUM_CARDINALITY			NUMBER(9,0),
	DTD_IDENTIFIER				VARCHAR(10485760),
	IS_SELF_REFERENCING			VARCHAR(10485760),
	IS_IDENTITY					VARCHAR(10485760),
	IDENTITY_GENERATION			VARCHAR(10485760),
	IDENTITY_START				VARCHAR(10485760),
	IDENTITY_INCREMENT			VARCHAR(10485760),
	IDENTITY_MAXIMUM			VARCHAR(10485760),
	IDENTITY_MINIMUM			VARCHAR(10485760),
	IDENTITY_CYCLE				VARCHAR(10485760),
	COMMENT						VARCHAR(10485760),
	ID_MODEL_RUN				VARCHAR(10485760) ,
	DT_MODEL_LOAD				VARCHAR(10485760) ,
	DT_LOAD 					TIMESTAMP  DEFAULT CURRENT_TIMESTAMP
);

create table if not exists RAW_DATA.ENVIRONMENTS (
	COD_ENVIRONMENT				VARCHAR(10485760),
	DESC_ENVIRONMENT				VARCHAR(10485760),
    WAREHOUSE_NAME				VARCHAR(10485760),
    ID_MODEL_RUN                VARCHAR(10485760),
	DT_MODEL_LOAD               VARCHAR(10485760),
	DT_LOAD                     VARCHAR(10485760)
);

create table if not exists RAW_DATA.ENVIRONMENT_CONNECTIONS (
	COD_ENVIRONMENT				VARCHAR(10485760),
	COD_CONNECTION         VARCHAR(10485760),
    DESC_ENVIRONMENT_CONNECTION				VARCHAR(10485760),
    ID_MODEL_RUN                VARCHAR(10485760),
	DT_MODEL_LOAD               VARCHAR(10485760),
	DT_LOAD                     VARCHAR(10485760)
);


create table if not exists RAW_DATA.MODEL (
	COD_MODEL				VARCHAR(10485760),
	DESC_MODEL				VARCHAR(10485760),
    COD_PROJECT             VARCHAR(10485760),
    ID_MODEL_RUN                VARCHAR(10485760),
	DT_MODEL_LOAD               VARCHAR(10485760),
	DT_LOAD                     VARCHAR(10485760)
);

create table if not exists RAW_DATA.DATASETS (
	COD_SETCONNECTION				VARCHAR(10485760),
	COD_DATASET				VARCHAR(10485760),
    COD_TYPE                VARCHAR(10485760),
    COD_MODEL               VARCHAR(10485760),
    COD_TYPE_LOAD_SOURCE    VARCHAR(10485760),
    COD_DATASET_ORIGIN      VARCHAR(10485760),
    COD_DATASET_NAME		VARCHAR(10485760),
	COD_DATASET_BUSINESS	VARCHAR(10485760),
    DESC_BUSINESS_DEFINITION	VARCHAR(10485760),
    ID_MODEL_RUN                VARCHAR(10485760),
	DT_MODEL_LOAD               VARCHAR(10485760),
	DT_LOAD                     VARCHAR(10485760)
);

create table if not exists RAW_DATA.DATASETS_SEGMENTS (
	COD_DATASET				VARCHAR(10485760),
	ID				VARCHAR(10485760),
    COD_DATASET_CHILD       VARCHAR(10485760),
    COD_DATASET_NAME        VARCHAR(10485760),
    DESC_BUSINESS_DEFINITION                    VARCHAR(10485760),
    ID_MODEL_RUN                VARCHAR(10485760),
	DT_MODEL_LOAD               VARCHAR(10485760),
	DT_LOAD                     VARCHAR(10485760)
);

create table if not exists RAW_DATA.RELATIONSHIPS (
	COD_RELATIONSHIP				VARCHAR(10485760),
	COD_RELATIONSHIP_NAME				VARCHAR(10485760),
    REFERENCE_COD_DATASET           VARCHAR(10485760),
    REFERENCE_COD_NAME              VARCHAR(10485760),
    TARGET_COD_DATASET              VARCHAR(10485760),
    TARGET_COD_NAME                 VARCHAR(10485760),
    TARGET_FINAL_COD_NAME           VARCHAR(10485760),
    DESC_BUSINESS_DEFINITION                            VARCHAR(10485760),
    ID_MODEL_RUN                VARCHAR(10485760),
	DT_MODEL_LOAD               VARCHAR(10485760),
	DT_LOAD                     VARCHAR(10485760)
);

create table if not exists RAW_DATA.DATASETS_FIELDS (
	COD_DATASET				VARCHAR(10485760),
	ID_ORDER				VARCHAR(10485760),
    COD_DATASET_FIELD       VARCHAR(10485760),
    COD_TYPE                VARCHAR(10485760),
    COD_COLUMN_NAME_TARGET  VARCHAR(10485760),
    DESC_BUSINESS_DEFINITION                    VARCHAR(10485760),
    ID_MODEL_RUN                VARCHAR(10485760),
	DT_MODEL_LOAD               VARCHAR(10485760),
	DT_LOAD                     VARCHAR(10485760)
);