DROP TABLE IF EXISTS GEN_PHASE.DATASETS_0;




create table if not exists GEN_PHASE.DATASETS_0 (
	COD_SETCONNECTION				VARCHAR(10485760),
	COD_DATASET				VARCHAR(10485760),
    COD_TYPE                VARCHAR(10485760),
    COD_MODEL               VARCHAR(10485760),
    COD_TYPE_LOAD_SOURCE    VARCHAR(10485760),
    COD_DATASET_ORIGIN      VARCHAR(10485760),
    DESC                    VARCHAR(10485760),
    ID_MODEL_RUN                VARCHAR(10485760),
	DT_MODEL_LOAD               VARCHAR(10485760),
	DT_LOAD                     VARCHAR(10485760)
);