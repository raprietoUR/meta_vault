{{ config(schema='gen_phase',
  alias='connections',
  materialized='incremental') }}

  
  

SELECT COD_CONNECTION COD_CONNECTION_BDK,COD_CONNECTION ||'||'||COD_AREA_PHASE||'||'||COD_TYPE_PHASE COD_CONNECTION,COD_AREA_PHASE,COD_TYPE_PHASE,
DES_CONNECTION,COD_PATH_0,COD_PATH_1,COD_PATH_2,COD_PATH_3
FROM  {{ ref('GEN_LOAD_CONNECTIONS_TMP') }}
