-- back compat for old kwarg name
  
  begin;
    

        insert into edw.gen_phase.datasets_0 ("COD_SETCONNECTION", "COD_DATASET", "COD_TYPE", "COD_MODEL", "COD_TYPE_LOAD_SOURCE", "COD_DATASET_ORIGIN", "DESC_BUSINESS_DEFINITION", "ID_MODEL_RUN", "DT_MODEL_LOAD", "DT_LOAD")
        (
            select "COD_SETCONNECTION", "COD_DATASET", "COD_TYPE", "COD_MODEL", "COD_TYPE_LOAD_SOURCE", "COD_DATASET_ORIGIN", "DESC_BUSINESS_DEFINITION", "ID_MODEL_RUN", "DT_MODEL_LOAD", "DT_LOAD"
            from edw.gen_phase.datasets_0__dbt_tmp
        );
    commit;