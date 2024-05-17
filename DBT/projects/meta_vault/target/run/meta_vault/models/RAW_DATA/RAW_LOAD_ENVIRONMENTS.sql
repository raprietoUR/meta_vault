-- back compat for old kwarg name
  
  begin;
    

        insert into edw.raw_data.environments ("COD_ENVIRONMENT", "DESC_ENVIRONMENT", "WAREHOUSE_NAME", "ID_MODEL_RUN", "DT_MODEL_LOAD", "DT_LOAD")
        (
            select "COD_ENVIRONMENT", "DESC_ENVIRONMENT", "WAREHOUSE_NAME", "ID_MODEL_RUN", "DT_MODEL_LOAD", "DT_LOAD"
            from edw.raw_data.environments__dbt_tmp
        );
    commit;