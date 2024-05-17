
  
    

        create or replace transient table edw.gen_phase.setconnection_connections
         as
        (

  
  
  
SELECT ST.COD_SETCONNECTION,CON.COD_CONNECTION,ST.DES_SETCONNECTION_CONNECTION DD,
REPLACE(ST.DES_SETCONNECTION_CONNECTION,CON.COD_CONNECTION_BDK||' Connection',CON.COD_CONNECTION||' Connection') DES_SETCONNECTION_CONNECTION,
FROM  edw.gen_phase.setconnection_connections_tmp ST
INNER JOIN  edw.gen_phase.connections CON
ON ST.COD_CONNECTION=CON.COD_CONNECTION_BDK
        );
      
  