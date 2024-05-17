{{ config(schema='gen_phase',
  alias='connections_tmp',
  materialized='incremental') }}

{% set cod_type_run = var("cod_type_run") %}
{% set cod_model = var("cod_model") %}



-- depends on:{{ ref('GEN_INIT') }}


SELECT TMP.COD_CONNECTION,TMP.COD_AREA_PHASE,TMP.COD_TYPE_PHASE,TMP.DES_CONNECTION,TMP.COD_PATH_0,
TMP.COD_PATH_1,TMP.COD_PATH_2,TMP.COD_PATH_3
FROM  {{ ref('RAW_LOAD_CONNECTIONS') }} TMP
INNER JOIN  {{ ref('GEN_REFRESH_MODEL_LOAD_RUNS_LAST') }} M
 ON TMP.ID_MODEL_RUN=M.ID_MODEL_RUN
WHERE
  M.COD_TYPE_RUN='{{ cod_type_run }}'
  AND M.COD_MODEL='{{ cod_model }}'
