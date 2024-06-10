{{ config(
    schema='gen_phase',
    alias='relationships',
    materialized='incremental'
) }}

-- depends on: {{ ref('GEN_INIT') }}


SELECT TMP.COD_RELATIONSHIP,TMP.COD_RELATIONSHIP_NAME
,TMP.REFERENCE_COD_DATASET,TMP.TARGET_COD_DATASET, TMP.DESC_BUSINESS_DEFINITION
FROM  {{ ref('RAW_LOAD_RELATIONSHIPS') }} TMP
{# INNER JOIN  {{ ref('GEN_REFRESH_MODEL_LOAD_RUNS_LAST') }} M
 ON TMP.ID_MODEL_RUN=M.ID_MODEL_RUN
 WHERE
M.COD_TYPE_RUN='{{ var('cod_type_run') }}'
AND M.COD_MODEL= '{{ var('cod_model') }}'  #}