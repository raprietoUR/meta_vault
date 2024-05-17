-- back compat for old kwarg name
  
  begin;
    

        insert into edw.gen_phase.setconnection_connections ("COD_SETCONNECTION", "COD_CONNECTION", "DD", "DES_SETCONNECTION_CONNECTION")
        (
            select "COD_SETCONNECTION", "COD_CONNECTION", "DD", "DES_SETCONNECTION_CONNECTION"
            from edw.gen_phase.setconnection_connections__dbt_tmp
        );
    commit;