-- back compat for old kwarg name
  
  begin;
    

        insert into edw.raw_data.datasets_fields ("COD_DATASET", "ID_ORDER", "COD_DATASET_FIELD", "COD_TYPE", "COD_COLUMN_NAME_TARGET", "DESC_BUSINESS_DEFINITION", "ID_MODEL_RUN", "DT_MODEL_LOAD", "DT_LOAD")
        (
            select "COD_DATASET", "ID_ORDER", "COD_DATASET_FIELD", "COD_TYPE", "COD_COLUMN_NAME_TARGET", "DESC_BUSINESS_DEFINITION", "ID_MODEL_RUN", "DT_MODEL_LOAD", "DT_LOAD"
            from edw.raw_data.datasets_fields__dbt_tmp
        );
    commit;