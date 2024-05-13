{{ config(
  alias='model_load_runs',
  materialized='incremental') }}

{% set cod_model =   var("cod_model") %}
{% set cod_type_run = var("cod_type_run") %}
{% set cod_environment = var("cod_environment") %}
{% set replace = var("replace") %}

    SELECT 	
    '{{ cod_type_run }}'||'-'||to_char(CURRENT_TIMESTAMP::TIMESTAMP, 'yyyymmddhh24missFF3') AS COD_RUN,
    '{{ cod_model }}' as COD_MODEL,
    to_varchar(CURRENT_TIMESTAMP::TIMESTAMPNTZ, 'yyyymmddhh24missFF3')  as DT_MODEL_LOAD,
    '{{ cod_type_run }}' AS COD_TYPE_RUN,
    '{{ cod_model }}'||'-'||'{{ cod_type_run }}'||'-'||to_char(CURRENT_TIMESTAMP::TIMESTAMP, 'yyyymmddhh24missFF3') AS DES_RUNS,
    MD5('{{ cod_type_run }}'||'-'||to_char(CURRENT_TIMESTAMP::TIMESTAMP, 'yyyymmddhh24missFF3')||'-'||'{{ cod_model }}') AS ID_MODEL_RUN
