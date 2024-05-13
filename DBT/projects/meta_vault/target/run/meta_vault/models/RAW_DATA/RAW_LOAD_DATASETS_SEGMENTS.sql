-- back compat for old kwarg name
  
  begin;
    

        insert into edw.RAW_DATA.datasets_segments ("COD_DATASET", "ID", "COD_DATASET_CHILD", "COD_DATASET_NAME", "DESC", "ID_MODEL_RUN", "DT_MODEL_LOAD", "DT_LOAD")
        (
            select "COD_DATASET", "ID", "COD_DATASET_CHILD", "COD_DATASET_NAME", "DESC", "ID_MODEL_RUN", "DT_MODEL_LOAD", "DT_LOAD"
            from edw.RAW_DATA.datasets_segments__dbt_tmp
        );
    commit;