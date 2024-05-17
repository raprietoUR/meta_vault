--Commands generated automatically by script csv_copy_into.py -- START
--COPY COMMAND ENVIRONMENT TABLES 
TRUNCATE TABLE RAW_DATA.SETCONNECTION_CONNECTIONS_TMP;
COPY INTO RAW_DATA.SETCONNECTION_CONNECTIONS_TMP FROM @~/UPLOADS/SETCONNECTION_CONNECTIONS.csv FILE_FORMAT = ( TYPE = CSV,  FIELD_OPTIONALLY_ENCLOSED_BY = '"' , SKIP_HEADER = 1, NULL_IF = ('[NULL]', '\N','NULL', 'NUL') );
TRUNCATE TABLE RAW_DATA.CONNECTIONS_TMP;
COPY INTO RAW_DATA.CONNECTIONS_TMP FROM @~/UPLOADS/CONNECTIONS.csv FILE_FORMAT = ( TYPE = CSV,  FIELD_OPTIONALLY_ENCLOSED_BY = '"' , SKIP_HEADER = 1, NULL_IF = ('[NULL]', '\N','NULL', 'NUL') );
TRUNCATE TABLE RAW_DATA.ENVIRONMENT_CONNECTIONS_TMP;
COPY INTO RAW_DATA.ENVIRONMENT_CONNECTIONS_TMP FROM @~/UPLOADS/ENVIRONMENT_CONNECTIONS.csv FILE_FORMAT = ( TYPE = CSV,  FIELD_OPTIONALLY_ENCLOSED_BY = '"' , SKIP_HEADER = 1, NULL_IF = ('[NULL]', '\N','NULL', 'NUL') );
TRUNCATE TABLE RAW_DATA.ENVIRONMENTS_TMP;
COPY INTO RAW_DATA.ENVIRONMENTS_TMP FROM @~/UPLOADS/ENVIRONMENTS.csv FILE_FORMAT = ( TYPE = CSV,  FIELD_OPTIONALLY_ENCLOSED_BY = '"' , SKIP_HEADER = 1, NULL_IF = ('[NULL]', '\N','NULL', 'NUL') );
--COPY COMMAND TABLES 
TRUNCATE TABLE RAW_DATA.RELATIONSHIPS_TMP;
COPY INTO RAW_DATA.RELATIONSHIPS_TMP FROM @~/UPLOADS/RELATIONSHIPS.csv FILE_FORMAT = ( TYPE = CSV,  FIELD_OPTIONALLY_ENCLOSED_BY = '"' , SKIP_HEADER = 1, NULL_IF = ('[NULL]', '\N','NULL', 'NUL') );
TRUNCATE TABLE RAW_DATA.DATASETS_TMP;
COPY INTO RAW_DATA.DATASETS_TMP FROM @~/UPLOADS/DATASETS.csv FILE_FORMAT = ( TYPE = CSV,  FIELD_OPTIONALLY_ENCLOSED_BY = '"' , SKIP_HEADER = 1, NULL_IF = ('[NULL]', '\N','NULL', 'NUL') );
TRUNCATE TABLE RAW_DATA.DATASETS_FIELDS_TMP;
COPY INTO RAW_DATA.DATASETS_FIELDS_TMP FROM @~/UPLOADS/DATASETS_FIELDS.csv FILE_FORMAT = ( TYPE = CSV,  FIELD_OPTIONALLY_ENCLOSED_BY = '"' , SKIP_HEADER = 1, NULL_IF = ('[NULL]', '\N','NULL', 'NUL') );
TRUNCATE TABLE RAW_DATA.MODEL_TMP;
COPY INTO RAW_DATA.MODEL_TMP FROM @~/UPLOADS/MODEL.csv FILE_FORMAT = ( TYPE = CSV,  FIELD_OPTIONALLY_ENCLOSED_BY = '"' , SKIP_HEADER = 1, NULL_IF = ('[NULL]', '\N','NULL', 'NUL') );
TRUNCATE TABLE RAW_DATA.DATASETS_SEGMENTS_TMP;
COPY INTO RAW_DATA.DATASETS_SEGMENTS_TMP FROM @~/UPLOADS/DATASETS_SEGMENTS.csv FILE_FORMAT = ( TYPE = CSV,  FIELD_OPTIONALLY_ENCLOSED_BY = '"' , SKIP_HEADER = 1, NULL_IF = ('[NULL]', '\N','NULL', 'NUL') );
--COPY COMMAND TABLES 
--Commands generated automatically by script csv_copy_into.py --END 
