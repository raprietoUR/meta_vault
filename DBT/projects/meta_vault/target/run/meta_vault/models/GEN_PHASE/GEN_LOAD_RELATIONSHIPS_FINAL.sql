
  
    

        create or replace transient table edw.gen_phase.relationships_final
         as
        (

SELECT
SEGMENT.COD_RELATIONSHIP_CHILD COD_RELATIONSHIP,SEGMENT.COD_RELATIONSHIP_NAME,
REL.REFERENCE_COD_DATASET,
REL.TARGET_COD_DATASET,ID_SEGMENT
,REL.DESC_BUSINESS_DEFINITION
FROM  edw.gen_phase.relationships_seg SEGMENT
INNER JOIN  edw.gen_phase.relationships REL
ON SEGMENT.COD_RELATIONSHIP=REL.COD_RELATIONSHIP
        );
      
  