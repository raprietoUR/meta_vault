{{ config(schema='tem_generator',
  materialized='table',
  
  post_hook=["DROP TABLE {{this}}"]) }}

--depends_on: {{ ref('GEN_END') }}

select null def
