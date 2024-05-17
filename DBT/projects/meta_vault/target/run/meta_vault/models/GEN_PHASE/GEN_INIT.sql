
  
    

        create or replace transient table edw.gen_phase.npsa_init
         as
        (

--depends_on: edw.raw_data.raw_end

select null def
        );
      
  