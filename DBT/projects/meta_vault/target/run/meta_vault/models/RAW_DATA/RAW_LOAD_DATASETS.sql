-- back compat for old kwarg name
  
  begin;
    

        insert into edw.RAW_DATA.datasets ("COD_SETCONNECTION", "COD_DATASET", "COD_TYPE", "COD_MODEL", "COD_TYPE_LOAD_SOURCE", "COD_DATASET_ORIGIN", "DESC", "ID_MODEL_RUN", "DT_MODEL_LOAD", "DT_LOAD")
        (
            select "COD_SETCONNECTION", "COD_DATASET", "COD_TYPE", "COD_MODEL", "COD_TYPE_LOAD_SOURCE", "COD_DATASET_ORIGIN", "DESC", "ID_MODEL_RUN", "DT_MODEL_LOAD", "DT_LOAD"
            from edw.RAW_DATA.datasets__dbt_tmp
        );
    commit;