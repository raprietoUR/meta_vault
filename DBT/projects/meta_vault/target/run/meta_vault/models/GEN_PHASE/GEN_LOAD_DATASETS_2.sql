
  
    

        create or replace transient table edw.gen_phase.datasets
         as
        (

SELECT * 
FROM  edw.gen_phase.datasets_0
	UNION ALL
	SELECT * 
  FROM  edw.gen_phase.datasets_1
        );
      
  