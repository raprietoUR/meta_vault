-- back compat for old kwarg name
  
  begin;
    

        insert into edw.raw_data.datasets_fields_columns ("TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "COLUMN_NAME", "ORDINAL_POSITION", "COLUMN_DEFAULT", "IS_NULLABLE", "DATA_TYPE", "CHARACTER_MAXIMUM_LENGTH", "CHARACTER_OCTET_LENGTH", "NUMERIC_PRECISION", "NUMERIC_PRECISION_RADIX", "NUMERIC_SCALE", "DATETIME_PRECISION", "INTERVAL_TYPE", "INTERVAL_PRECISION", "CHARACTER_SET_CATALOG", "CHARACTER_SET_SCHEMA", "CHARACTER_SET_NAME", "COLLATION_CATALOG", "COLLATION_SCHEMA", "COLLATION_NAME", "DOMAIN_CATALOG", "DOMAIN_SCHEMA", "DOMAIN_NAME", "UDT_CATALOG", "UDT_SCHEMA", "UDT_NAME", "SCOPE_CATALOG", "SCOPE_SCHEMA", "SCOPE_NAME", "MAXIMUM_CARDINALITY", "DTD_IDENTIFIER", "IS_SELF_REFERENCING", "IS_IDENTITY", "IDENTITY_GENERATION", "IDENTITY_START", "IDENTITY_INCREMENT", "IDENTITY_MAXIMUM", "IDENTITY_MINIMUM", "IDENTITY_CYCLE", "COMMENT", "ID_MODEL_RUN", "DT_MODEL_LOAD", "DT_LOAD")
        (
            select "TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "COLUMN_NAME", "ORDINAL_POSITION", "COLUMN_DEFAULT", "IS_NULLABLE", "DATA_TYPE", "CHARACTER_MAXIMUM_LENGTH", "CHARACTER_OCTET_LENGTH", "NUMERIC_PRECISION", "NUMERIC_PRECISION_RADIX", "NUMERIC_SCALE", "DATETIME_PRECISION", "INTERVAL_TYPE", "INTERVAL_PRECISION", "CHARACTER_SET_CATALOG", "CHARACTER_SET_SCHEMA", "CHARACTER_SET_NAME", "COLLATION_CATALOG", "COLLATION_SCHEMA", "COLLATION_NAME", "DOMAIN_CATALOG", "DOMAIN_SCHEMA", "DOMAIN_NAME", "UDT_CATALOG", "UDT_SCHEMA", "UDT_NAME", "SCOPE_CATALOG", "SCOPE_SCHEMA", "SCOPE_NAME", "MAXIMUM_CARDINALITY", "DTD_IDENTIFIER", "IS_SELF_REFERENCING", "IS_IDENTITY", "IDENTITY_GENERATION", "IDENTITY_START", "IDENTITY_INCREMENT", "IDENTITY_MAXIMUM", "IDENTITY_MINIMUM", "IDENTITY_CYCLE", "COMMENT", "ID_MODEL_RUN", "DT_MODEL_LOAD", "DT_LOAD"
            from edw.raw_data.datasets_fields_columns__dbt_tmp
        );
    commit;