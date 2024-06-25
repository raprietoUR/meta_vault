
  
    

        create or replace transient table edw.tem_generator.tem_init
         as
        (

--depends_on: edw.gen_phase.npsa_end

select null def
        );
      
  