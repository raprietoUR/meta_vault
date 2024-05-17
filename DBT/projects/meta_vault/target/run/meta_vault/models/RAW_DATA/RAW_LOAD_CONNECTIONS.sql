-- back compat for old kwarg name
  
  begin;
    

        insert into edw.raw_data.connections ("COD_CONNECTION", "COD_AREA_PHASE", "COD_TYPE_PHASE", "DES_CONNECTION", "COD_PATH_0", "COD_PATH_1", "COD_PATH_2", "COD_PATH_3", "ID_MODEL_RUN", "DT_MODEL_LOAD", "DT_LOAD")
        (
            select "COD_CONNECTION", "COD_AREA_PHASE", "COD_TYPE_PHASE", "DES_CONNECTION", "COD_PATH_0", "COD_PATH_1", "COD_PATH_2", "COD_PATH_3", "ID_MODEL_RUN", "DT_MODEL_LOAD", "DT_LOAD"
            from edw.raw_data.connections__dbt_tmp
        );
    commit;