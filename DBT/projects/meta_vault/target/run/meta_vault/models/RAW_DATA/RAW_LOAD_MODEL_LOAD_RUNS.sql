-- back compat for old kwarg name
  
  begin;
    

        insert into edw.raw_data.model_load_runs ("COD_RUN", "COD_MODEL", "DT_MODEL_LOAD", "COD_TYPE_RUN", "DES_RUNS", "ID_MODEL_RUN")
        (
            select "COD_RUN", "COD_MODEL", "DT_MODEL_LOAD", "COD_TYPE_RUN", "DES_RUNS", "ID_MODEL_RUN"
            from edw.raw_data.model_load_runs__dbt_tmp
        );
    commit;