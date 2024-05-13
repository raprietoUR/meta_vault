-- back compat for old kwarg name
  
  begin;
    

        insert into edw.RAW_DATA.model ("COD_MODEL", "DESC_MODEL", "COD_PROJECT", "ID_MODEL_RUN", "DT_MODEL_LOAD", "DT_LOAD")
        (
            select "COD_MODEL", "DESC_MODEL", "COD_PROJECT", "ID_MODEL_RUN", "DT_MODEL_LOAD", "DT_LOAD"
            from edw.RAW_DATA.model__dbt_tmp
        );
    commit;