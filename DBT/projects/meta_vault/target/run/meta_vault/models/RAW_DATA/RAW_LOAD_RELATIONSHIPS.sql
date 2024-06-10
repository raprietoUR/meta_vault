-- back compat for old kwarg name
  
  begin;
    

        insert into edw.raw_data.relationships ("COD_RELATIONSHIP", "COD_RELATIONSHIP_NAME", "REFERENCE_COD_DATASET", "TARGET_COD_DATASET", "DESC_BUSINESS_DEFINITION", "ID_MODEL_RUN", "DT_MODEL_LOAD", "DT_LOAD")
        (
            select "COD_RELATIONSHIP", "COD_RELATIONSHIP_NAME", "REFERENCE_COD_DATASET", "TARGET_COD_DATASET", "DESC_BUSINESS_DEFINITION", "ID_MODEL_RUN", "DT_MODEL_LOAD", "DT_LOAD"
            from edw.raw_data.relationships__dbt_tmp
        );
    commit;