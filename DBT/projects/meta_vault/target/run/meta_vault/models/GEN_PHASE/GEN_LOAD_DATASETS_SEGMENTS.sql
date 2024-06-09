-- back compat for old kwarg name
  
  begin;
    

        insert into edw.gen_phase.datasets_segments ("ID_MODEL_RUN", "COD_DATASET", "ID_SEGMENT", "COD_DATASET_CHILD", "COD_DATASET_NAME", "DESC_BUSINESS_DEFINITION")
        (
            select "ID_MODEL_RUN", "COD_DATASET", "ID_SEGMENT", "COD_DATASET_CHILD", "COD_DATASET_NAME", "DESC_BUSINESS_DEFINITION"
            from edw.gen_phase.datasets_segments__dbt_tmp
        );
    commit;