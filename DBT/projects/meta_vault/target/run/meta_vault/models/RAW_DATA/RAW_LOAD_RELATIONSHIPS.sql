-- back compat for old kwarg name
  
  begin;
    

        insert into edw.RAW_DATA.relationships ("COD_RELATIONSHIP", "COD_RELATIONSHIP_NAME", "REFERENCE_COD_DATASET", "REFERENCE_COD_NAME", "TARGET_COD_DATASET", "TARGET_COD_NAME", "TARGET_FINAL_COD_NAME", "DESC", "ID_MODEL_RUN", "DT_MODEL_LOAD", "DT_LOAD")
        (
            select "COD_RELATIONSHIP", "COD_RELATIONSHIP_NAME", "REFERENCE_COD_DATASET", "REFERENCE_COD_NAME", "TARGET_COD_DATASET", "TARGET_COD_NAME", "TARGET_FINAL_COD_NAME", "DESC", "ID_MODEL_RUN", "DT_MODEL_LOAD", "DT_LOAD"
            from edw.RAW_DATA.relationships__dbt_tmp
        );
    commit;