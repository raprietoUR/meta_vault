-- back compat for old kwarg name
  
  begin;
    

        insert into edw.gen_phase.environment_connections ("COD_ENVIRONMENT", "COD_CONNECTION", "DES_ENVIRONMENT_CONNECTION")
        (
            select "COD_ENVIRONMENT", "COD_CONNECTION", "DES_ENVIRONMENT_CONNECTION"
            from edw.gen_phase.environment_connections__dbt_tmp
        );
    commit;