
  
    

        create or replace transient table edw.gen_phase.environment_connections
         as
        (

  
  
  
SELECT ENV.COD_ENVIRONMENT,CON.COD_CONNECTION,REPLACE(ENV.DESC_ENVIRONMENT_CONNECTION,CON.COD_CONNECTION_BDK,CON.COD_CONNECTION) DES_ENVIRONMENT_CONNECTION
FROM  edw.gen_phase.environment_connections_tmp ENV
INNER JOIN  edw.gen_phase.connections CON
ON ENV.COD_CONNECTION=CON.COD_CONNECTION_BDK
        );
      
  