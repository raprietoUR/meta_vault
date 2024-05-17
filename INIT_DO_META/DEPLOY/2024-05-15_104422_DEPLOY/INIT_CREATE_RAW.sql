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
	TABLE_CATALOG				{{varchar_max}},
	TABLE_SCHEMA				{{varchar_max}},
	TABLE_NAME					{{varchar_max}},
	COLUMN_NAME					{{varchar_max}},
	ORDINAL_POSITION			{{varchar_max}},
	COLUMN_DEFAULT				{{varchar_max}},
	IS_NULLABLE					{{varchar_max}},
	DATA_TYPE					{{varchar_max}},
	CHARACTER_MAXIMUM_LENGTH	{{varchar_max}},
	CHARACTER_OCTET_LENGTH		{{varchar_max}},
	NUMERIC_PRECISION			{{varchar_max}},
	NUMERIC_PRECISION_RADIX		{{varchar_max}},
	NUMERIC_SCALE				{{varchar_max}},
	DATETIME_PRECISION			{{varchar_max}},
	INTERVAL_TYPE				{{varchar_max}},
	INTERVAL_PRECISION			{{varchar_max}},
	CHARACTER_SET_CATALOG		{{varchar_max}},
	CHARACTER_SET_SCHEMA		{{varchar_max}},
	CHARACTER_SET_NAME			{{varchar_max}},
	COLLATION_CATALOG			{{varchar_max}},
	COLLATION_SCHEMA			{{varchar_max}},
	COLLATION_NAME				{{varchar_max}},
	DOMAIN_CATALOG				{{varchar_max}},
	DOMAIN_SCHEMA				{{varchar_max}},
	DOMAIN_NAME					{{varchar_max}},
	UDT_CATALOG					{{varchar_max}},
	UDT_SCHEMA					{{varchar_max}},
	UDT_NAME					{{varchar_max}},
	SCOPE_CATALOG				{{varchar_max}},
	SCOPE_SCHEMA				{{varchar_max}},
	SCOPE_NAME					{{varchar_max}},
	MAXIMUM_CARDINALITY			{{varchar_max}},
	DTD_IDENTIFIER				{{varchar_max}},
	IS_SELF_REFERENCING			{{varchar_max}},
	IS_IDENTITY					{{varchar_max}},
	IDENTITY_GENERATION			{{varchar_max}},
	IDENTITY_START				{{varchar_max}},
	IDENTITY_INCREMENT			{{varchar_max}},
	IDENTITY_MAXIMUM			{{varchar_max}},
	IDENTITY_MINIMUM			{{varchar_max}},
	IDENTITY_CYCLE				{{varchar_max}},
	COMMENT						{{varchar_max}}
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
    DESC                    VARCHAR(10485760)
);

create table if not exists RAW_DATA.DATASETS_SEGMENTS_TMP (
	COD_DATASET				VARCHAR(10485760),
	ID				VARCHAR(10485760),
    COD_DATASET_CHILD       VARCHAR(10485760),
    COD_DATASET_NAME        VARCHAR(10485760),
    DESC                    VARCHAR(10485760)
);

create table if not exists RAW_DATA.RELATIONSHIPS_TMP (
	COD_RELATIONSHIP				VARCHAR(10485760),
	COD_RELATIONSHIP_NAME				VARCHAR(10485760),
    REFERENCE_COD_DATASET           VARCHAR(10485760),
    REFERENCE_COD_NAME              VARCHAR(10485760),
    TARGET_COD_DATASET              VARCHAR(10485760),
    TARGET_COD_NAME                 VARCHAR(10485760),
    TARGET_FINAL_COD_NAME           VARCHAR(10485760),
    DESC                            VARCHAR(10485760)
);

create table if not exists RAW_DATA.DATASETS_FIELDS_TMP (
	COD_DATASET				VARCHAR(10485760),
	ID_ORDER				VARCHAR(10485760),
    COD_DATASET_FIELD       VARCHAR(10485760),
    COD_TYPE                VARCHAR(10485760),
    COD_COLUMN_NAME_TARGET  VARCHAR(10485760),
    DESC                    VARCHAR(10485760)
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
	TABLE_CATALOG				{{varchar_max}},
	TABLE_SCHEMA				{{varchar_max}},
	TABLE_NAME					{{varchar_max}},
	COLUMN_NAME					{{varchar_max}},
	ORDINAL_POSITION			{{varchar_max}},
	COLUMN_DEFAULT				{{varchar_max}},
	IS_NULLABLE					{{varchar_max}},
	DATA_TYPE					{{varchar_max}},
	CHARACTER_MAXIMUM_LENGTH	{{varchar_max}},
	CHARACTER_OCTET_LENGTH		{{varchar_max}},
	NUMERIC_PRECISION			{{varchar_max}},
	NUMERIC_PRECISION_RADIX		{{varchar_max}},
	NUMERIC_SCALE				{{varchar_max}},
	DATETIME_PRECISION			{{varchar_max}},
	INTERVAL_TYPE				{{varchar_max}},
	INTERVAL_PRECISION			{{varchar_max}},
	CHARACTER_SET_CATALOG		{{varchar_max}},
	CHARACTER_SET_SCHEMA		{{varchar_max}},
	CHARACTER_SET_NAME			{{varchar_max}},
	COLLATION_CATALOG			{{varchar_max}},
	COLLATION_SCHEMA			{{varchar_max}},
	COLLATION_NAME				{{varchar_max}},
	DOMAIN_CATALOG				{{varchar_max}},
	DOMAIN_SCHEMA				{{varchar_max}},
	DOMAIN_NAME					{{varchar_max}},
	UDT_CATALOG					{{varchar_max}},
	UDT_SCHEMA					{{varchar_max}},
	UDT_NAME					{{varchar_max}},
	SCOPE_CATALOG				{{varchar_max}},
	SCOPE_SCHEMA				{{varchar_max}},
	SCOPE_NAME					{{varchar_max}},
	MAXIMUM_CARDINALITY			{{varchar_max}},
	DTD_IDENTIFIER				{{varchar_max}},
	IS_SELF_REFERENCING			{{varchar_max}},
	IS_IDENTITY					{{varchar_max}},
	IDENTITY_GENERATION			{{varchar_max}},
	IDENTITY_START				{{varchar_max}},
	IDENTITY_INCREMENT			{{varchar_max}},
	IDENTITY_MAXIMUM			{{varchar_max}},
	IDENTITY_MINIMUM			{{varchar_max}},
	IDENTITY_CYCLE				{{varchar_max}},
	COMMENT						{{varchar_max}}
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
    DESC                    VARCHAR(10485760),
    ID_MODEL_RUN                VARCHAR(10485760),
	DT_MODEL_LOAD               VARCHAR(10485760),
	DT_LOAD                     VARCHAR(10485760)
);

create table if not exists RAW_DATA.DATASETS_SEGMENTS (
	COD_DATASET				VARCHAR(10485760),
	ID				VARCHAR(10485760),
    COD_DATASET_CHILD       VARCHAR(10485760),
    COD_DATASET_NAME        VARCHAR(10485760),
    DESC                    VARCHAR(10485760),
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
    DESC                            VARCHAR(10485760),
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
    DESC                    VARCHAR(10485760),
    ID_MODEL_RUN                VARCHAR(10485760),
	DT_MODEL_LOAD               VARCHAR(10485760),
	DT_LOAD                     VARCHAR(10485760)
);