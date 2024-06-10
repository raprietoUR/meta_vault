
  
    

        create or replace transient table edw.gen_phase.datasets_fields
         as
        (

SELECT DISTINCT * 
FROM  edw.gen_phase.datasets_fields_3 
UNION ALL
SELECT DISTINCT * 
FROM  edw.gen_phase.datasets_fields_4
        );
      
  