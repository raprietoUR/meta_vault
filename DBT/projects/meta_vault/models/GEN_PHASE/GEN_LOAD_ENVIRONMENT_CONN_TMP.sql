{{ config(schema='gen_phase',
  alias='environment_connections_tmp',
  materialized='incremental') }}

  
  

-- depends on: {{ ref('GEN_INIT') }}


SELECT TMP.COD_ENVIRONMENT,TMP.COD_CONNECTION,TMP.DESC_ENVIRONMENT_CONNECTION
FROM  {{ ref('RAW_LOAD_ENVIRONMENT_CONNECTIONS') }} TMP
INNER JOIN  {{ ref('GEN_REFRESH_MODEL_LOAD_RUNS_LAST') }} M
 ON TMP.ID_MODEL_RUN=M.ID_MODEL_RUN
WHERE
M.COD_TYPE_RUN='{{ var('cod_type_run') }}'
AND M.COD_MODEL= '{{ var('cod_model') }}'
