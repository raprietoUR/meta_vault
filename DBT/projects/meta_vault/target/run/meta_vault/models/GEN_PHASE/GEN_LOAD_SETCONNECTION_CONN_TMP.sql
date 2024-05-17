-- back compat for old kwarg name
  
  begin;
    

        insert into edw.gen_phase.setconnection_connections_tmp ("COD_SETCONNECTION", "COD_CONNECTION", "DES_SETCONNECTION_CONNECTION")
        (
            select "COD_SETCONNECTION", "COD_CONNECTION", "DES_SETCONNECTION_CONNECTION"
            from edw.gen_phase.setconnection_connections_tmp__dbt_tmp
        );
    commit;