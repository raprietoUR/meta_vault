{{ config(schema='gen_phase',
  alias='model_load_runs_last',
  materialized='view') }}

-- depends_on: {{ ref('GEN_INIT')}}


{% set cod_model =   var("cod_model") %}
{% set cod_type_run = var("cod_type_run") %}
{% set cod_environment = var("cod_environment") %}
{% set replace = var("replace") %}

SELECT * from
(select COD_RUN,
        COD_MODEL,
        COD_TYPE_RUN,
        DES_RUNS,
  {% if target.type == "snowflake" %}
        to_timestamp(DT_MODEL_LOAD,'yyyymmddhh24missFF3') DT_MODEL_LOAD,
  {% else %}
        to_timestamp(DT_MODEL_LOAD,'yyyymmddhh24missMS') DT_MODEL_LOAD,
  {% endif %}
        DT_MODEL_LOAD DT_START,
        coalesce(LAG(DT_MODEL_LOAD,1) OVER (PARTITION BY COD_MODEL,COD_TYPE_RUN ORDER BY DT_MODEL_LOAD DESC), '01/01/2020') DT_END,
        ROW_NUMBER() OVER (PARTITION BY COD_MODEL,COD_TYPE_RUN ORDER BY DT_MODEL_LOAD DESC) ID_ORDER,
        ID_MODEL_RUN
    FROM  {{ ref('RAW_LOAD_MODEL_LOAD_RUNS') }} 
    WHERE COD_MODEL = '{{ cod_model }}'
    AND COD_TYPE_RUN = '{{ cod_type_run }}'
    ) MLR
    where ID_ORDER = 1
