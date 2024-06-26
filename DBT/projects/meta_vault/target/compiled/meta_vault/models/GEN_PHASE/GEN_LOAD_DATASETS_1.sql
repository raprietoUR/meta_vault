

SELECT DS.COD_SETCONNECTION,
	SEG.COD_DATASET_CHILD COD_DATASET,
	SEG.COD_DATASET_NAME,
	CASE WHEN DS.COD_TYPE_ENTITY IN ('ENTITY') THEN 'SAT'
		ELSE 'SATL' END COD_TYPE,
	DS.COD_MODEL,
	DS.SW_AUTOMATED_DV,
	DS.COD_TYPE_LOAD_SOURCE,
	DS.COD_DATASET_ORIGIN,
	DS.COD_TYPE_ENTITY,
	SEG.DESC_BUSINESS_DEFINITION DESC_DATASET,
	DS.ID_MODEL_RUN,
	DS.DT_MODEL_LOAD,	
	DS.DT_LOAD,
	1 SW_INCREMENTAL,
	0 SW_RECREATE,
FROM  edw.gen_phase.datasets_0 DS
INNER JOIN  edw.gen_phase.datasets_segments SEG
ON DS.COD_DATASET = SEG.COD_DATASET