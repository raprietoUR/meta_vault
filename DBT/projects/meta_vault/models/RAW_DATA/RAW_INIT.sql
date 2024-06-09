{{ config(schema='raw_data',
  materialized='table',
  post_hook=["DROP TABLE {{this}}"]) }}


select null def
