
  
    

        create or replace transient table edw.gen_phase.TEM_INIT
         as
        (

--depends_on: edw.gen_phase.npsa_end

select null def
        );
      
  