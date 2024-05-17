-- back compat for old kwarg name
  
  begin;
    

        insert into edw.raw_data.datasets ("COD_SETCONNECTION", "COD_DATASET", "COD_TYPE", "COD_MODEL", "COD_TYPE_LOAD_SOURCE", "COD_DATASET_ORIGIN", "COD_DATASET_NAME", "COD_DATASET_BUSINESS", "DESC_BUSINESS_DEFINITION", "ID_MODEL_RUN", "DT_MODEL_LOAD", "DT_LOAD")
        (
            select "COD_SETCONNECTION", "COD_DATASET", "COD_TYPE", "COD_MODEL", "COD_TYPE_LOAD_SOURCE", "COD_DATASET_ORIGIN", "COD_DATASET_NAME", "COD_DATASET_BUSINESS", "DESC_BUSINESS_DEFINITION", "ID_MODEL_RUN", "DT_MODEL_LOAD", "DT_LOAD"
            from edw.raw_data.datasets__dbt_tmp
        );
    commit;