{{ config(schema='gen_phase',
  materialized='table',
  
  post_hook=["DROP TABLE {{this}}"]) }}

--depends_on: {{ ref('GEN_LOAD_ELEMENTS') }}


select null def
