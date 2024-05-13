-- back compat for old kwarg name
  
  begin;
    

        insert into edw.RAW_DATA.environment_connections ("COD_ENVIRONMENT", "COD_TYPE_CONNECTION", "DESC_ENVIRONMENT_CONNECTION", "ID_MODEL_RUN", "DT_MODEL_LOAD", "DT_LOAD")
        (
            select "COD_ENVIRONMENT", "COD_TYPE_CONNECTION", "DESC_ENVIRONMENT_CONNECTION", "ID_MODEL_RUN", "DT_MODEL_LOAD", "DT_LOAD"
            from edw.RAW_DATA.environment_connections__dbt_tmp
        );
    commit;