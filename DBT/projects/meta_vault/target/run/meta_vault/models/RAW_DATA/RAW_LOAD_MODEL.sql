-- back compat for old kwarg name
  
  begin;
    

        insert into edw.raw_data.model ("COD_MODEL", "DESC_MODEL", "COD_PROJECT", "ID_MODEL_RUN", "DT_MODEL_LOAD", "DT_LOAD")
        (
            select "COD_MODEL", "DESC_MODEL", "COD_PROJECT", "ID_MODEL_RUN", "DT_MODEL_LOAD", "DT_LOAD"
            from edw.raw_data.model__dbt_tmp
        );
    commit;