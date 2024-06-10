
  
    

        create or replace transient table edw.gen_phase.datasets_fields_3
         as
        (

SELECT * 
FROM  edw.gen_phase.datasets_fields_1 
UNION ALL
SELECT * 
FROM  edw.gen_phase.datasets_fields_2
        );
      
  