{{ config(schema='gen_phase',
  alias='environment_connections',
  materialized='incremental') }}

  
  
  
SELECT ENV.COD_ENVIRONMENT,CON.COD_CONNECTION,REPLACE(ENV.DESC_ENVIRONMENT_CONNECTION,CON.COD_CONNECTION_BDK,CON.COD_CONNECTION) DES_ENVIRONMENT_CONNECTION
FROM  {{ ref('GEN_LOAD_ENVIRONMENT_CONN_TMP') }} ENV
INNER JOIN  {{ ref('GEN_LOAD_CONNECTIONS') }} CON
ON ENV.COD_CONNECTION=CON.COD_CONNECTION_BDK
