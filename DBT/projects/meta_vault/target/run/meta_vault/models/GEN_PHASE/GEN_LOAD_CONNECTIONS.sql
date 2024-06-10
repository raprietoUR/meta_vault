
  
    

        create or replace transient table edw.gen_phase.connections
         as
        (

  
  

SELECT COD_CONNECTION COD_CONNECTION_BDK,COD_CONNECTION ||'||'||COD_AREA_PHASE||'||'||COD_TYPE_PHASE COD_CONNECTION,COD_AREA_PHASE,COD_TYPE_PHASE,
DES_CONNECTION,COD_PATH_0,COD_PATH_1,COD_PATH_2,COD_PATH_3
FROM  edw.gen_phase.connections_tmp
        );
      
  