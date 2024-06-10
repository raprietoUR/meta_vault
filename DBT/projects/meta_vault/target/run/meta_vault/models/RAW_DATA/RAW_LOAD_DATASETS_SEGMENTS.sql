-- back compat for old kwarg name
  
  begin;
    

        insert into edw.raw_data.datasets_segments ("ID_TYPE_SEGMENT", "COD_DATASET", "ID", "COD_DATASET_CHILD", "COD_DATASET_NAME", "DESC_BUSINESS_DEFINITION", "ID_MODEL_RUN", "DT_MODEL_LOAD", "DT_LOAD")
        (
            select "ID_TYPE_SEGMENT", "COD_DATASET", "ID", "COD_DATASET_CHILD", "COD_DATASET_NAME", "DESC_BUSINESS_DEFINITION", "ID_MODEL_RUN", "DT_MODEL_LOAD", "DT_LOAD"
            from edw.raw_data.datasets_segments__dbt_tmp
        );
    commit;