{{ config(
  materialized='table',
  
  post_hook=["DROP TABLE {{this}}"]) }}


select null def
