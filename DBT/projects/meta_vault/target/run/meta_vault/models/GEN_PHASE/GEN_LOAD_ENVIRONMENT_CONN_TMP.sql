-- back compat for old kwarg name
  
  begin;
    

        insert into edw.gen_phase.environment_connections_tmp ("COD_ENVIRONMENT", "COD_CONNECTION", "DESC_ENVIRONMENT_CONNECTION")
        (
            select "COD_ENVIRONMENT", "COD_CONNECTION", "DESC_ENVIRONMENT_CONNECTION"
            from edw.gen_phase.environment_connections_tmp__dbt_tmp
        );
    commit;