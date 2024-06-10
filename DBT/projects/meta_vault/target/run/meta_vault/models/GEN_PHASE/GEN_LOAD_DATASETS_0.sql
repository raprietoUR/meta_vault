
  
    

        create or replace transient table edw.gen_phase.datasets_0
         as
        (

SELECT COD_SETCONNECTION
	, COD_DATASET	
	, COD_TYPE	
	, COD_MODEL	
	, SW_AUTOMATED_DV
	, COD_TYPE_LOAD_SOURCE	
	, COD_DATASET_ORIGIN	
	, COD_TYPE_ENTITY
	, DESC_BUSINESS_DEFINITION
	, ID_MODEL_RUN	
	, DT_MODEL_LOAD	
	, DT_LOAD
	, 1 SW_INCREMENTAL
	, 0 SW_RECREATE
FROM edw.raw_data.datasets
        );
      
  