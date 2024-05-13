-- back compat for old kwarg name
  
  begin;
    

        insert into edw.RAW_DATA.datasets_fields ("COD_DATASET", "IR_ORDER", "COD_DATASET_FIELD", "COD_TYPE", "COD_COLUMN_NAME_TARGET", "DESC", "ID_MODEL_RUN", "DT_MODEL_LOAD", "DT_LOAD")
        (
            select "COD_DATASET", "IR_ORDER", "COD_DATASET_FIELD", "COD_TYPE", "COD_COLUMN_NAME_TARGET", "DESC", "ID_MODEL_RUN", "DT_MODEL_LOAD", "DT_LOAD"
            from edw.RAW_DATA.datasets_fields__dbt_tmp
        );
    commit;