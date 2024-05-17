
  
    

        create or replace transient table edw.raw_data.raw_end
         as
        (

--depends_on: edw.raw_data.datasets,edw.raw_data.datasets_fields,edw.raw_data.datasets_segments,edw.raw_data.environment_connections,edw.raw_data.environments,edw.raw_data.model,edw.raw_data.relationships


select null def
        );
      
  