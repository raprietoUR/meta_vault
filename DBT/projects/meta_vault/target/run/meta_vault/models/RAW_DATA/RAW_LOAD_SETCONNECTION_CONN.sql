-- back compat for old kwarg name
  
  begin;
    

        insert into edw.raw_data.setconnection_connections ("COD_SETCONNECTION", "COD_CONNECTION", "DES_SETCONNECTION_CONNECTION", "ID_MODEL_RUN", "DT_MODEL_LOAD", "DT_LOAD")
        (
            select "COD_SETCONNECTION", "COD_CONNECTION", "DES_SETCONNECTION_CONNECTION", "ID_MODEL_RUN", "DT_MODEL_LOAD", "DT_LOAD"
            from edw.raw_data.setconnection_connections__dbt_tmp
        );
    commit;