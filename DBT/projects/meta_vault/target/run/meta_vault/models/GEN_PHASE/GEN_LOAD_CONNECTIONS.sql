-- back compat for old kwarg name
  
  begin;
    

        insert into edw.gen_phase.connections ("COD_CONNECTION_BDK", "COD_CONNECTION", "COD_AREA_PHASE", "COD_TYPE_PHASE", "DES_CONNECTION", "COD_PATH_0", "COD_PATH_1", "COD_PATH_2", "COD_PATH_3")
        (
            select "COD_CONNECTION_BDK", "COD_CONNECTION", "COD_AREA_PHASE", "COD_TYPE_PHASE", "DES_CONNECTION", "COD_PATH_0", "COD_PATH_1", "COD_PATH_2", "COD_PATH_3"
            from edw.gen_phase.connections__dbt_tmp
        );
    commit;