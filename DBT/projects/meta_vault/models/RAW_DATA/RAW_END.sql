{{ config(schema='raw_data',
  materialized='table',
  
  post_hook=["DROP TABLE {{this}}"]) }}

--depends_on: {{ ref('RAW_LOAD_DATASETS') }},{{ ref('RAW_LOAD_DATASETS_FIELDS') }},{{ ref('RAW_LOAD_DATASETS_SEGMENTS') }},{{ ref('RAW_LOAD_ENVIRONMENT_CONNECTIONS') }},{{ ref('RAW_LOAD_ENVIRONMENTS') }},{{ ref('RAW_LOAD_MODEL') }},{{ ref('RAW_LOAD_RELATIONSHIPS') }}


select null def
