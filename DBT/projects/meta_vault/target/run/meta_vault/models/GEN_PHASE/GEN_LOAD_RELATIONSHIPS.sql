
  
    

        create or replace transient table edw.gen_phase.relationships
         as
        (

-- depends on: edw.gen_phase.npsa_init


SELECT TMP.COD_RELATIONSHIP,TMP.COD_RELATIONSHIP_NAME
,TMP.REFERENCE_COD_DATASET,TMP.TARGET_COD_DATASET, TMP.DESC_BUSINESS_DEFINITION
FROM  edw.raw_data.relationships TMP

        );
      
  