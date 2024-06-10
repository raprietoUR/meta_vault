-- back compat for old kwarg name
  
  begin;
    

        insert into edw.gen_phase.datasets_fields_0 ("COD_DATASET", "ID_ORDER", "COD_DATASET_FIELD", "COD_TYPE", "COD_COLUMN_NAME_TARGET", "SW_TRACK_DIFF", "COLUMN_VALUE_DEFAULT_FX", "SW_VIRTUAL_FIELD_VALUE", "DESC_BUSINESS_DESCRIPTION")
        (
            select "COD_DATASET", "ID_ORDER", "COD_DATASET_FIELD", "COD_TYPE", "COD_COLUMN_NAME_TARGET", "SW_TRACK_DIFF", "COLUMN_VALUE_DEFAULT_FX", "SW_VIRTUAL_FIELD_VALUE", "DESC_BUSINESS_DESCRIPTION"
            from edw.gen_phase.datasets_fields_0__dbt_tmp
        );
    commit;