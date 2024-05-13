"""
To see the help, use the next command:

    python generate_sql_put_copy.py --help

"""

import pathlib
import os
from pyexpat import model
import sys
from os.path import exists
import shutil
from datetime import datetime
import time
import csv
import pandas as pd

import os, fnmatch

def find(pattern, path):
    result = []
    for root, dirs, files in os.walk(path):
        for name in files:
            if fnmatch.fnmatch(name, pattern):
                result = name
    return result

####################################################### VARIABLE DEFINITION
sql_dir = ""
sql_dir = ""
put_sql= "DV_FW_Snowflake_puts.sql"
copy_sql= "DV_FW_copy.sql"
df_out_dir = "DEPLOY/"
v_exec = ""
v_stage ="@DVFW/"
put_sql_txt = ""
copy_sql_txt = ""
exec = ""
script_def = "generate_sql_put_copy.py"



####################################################### VARIABLE DEFINITION

if "--help" in sys.argv:
    help = "Documentation: \r\nUse:\r\n\tpython generate_sql_put_copy.py [-t] | [-uc] snowflake|postgresql -d DIR \r\n\r\n"
    help += "Options:\n"
    help += "\tsnowflake    Creates the .sql file that will be used to 'put' the files into the Snowflake internal storate and the .sql file to do the 'copy into' the tables.\r\n"
    help += "\tpostgresql   Creates the .sql file that will be used to 'copy' the files into the tables.\r\n"
    print(help)
    exit(0)

print("[", datetime.now() , "] [", script_def, "] Generation of sql process Started")


#VALIDATE INPUT PARAMETERS
if ("-uc" not in sys.argv) and ("-t" not in sys.argv):
        print("--> ERROR. Any TRONCAL (-t) or USE CASE (-uc) must be specified")        
        exit(1)

if ("-d" not in sys.argv ):
        print("--> ERROR. Output directory is mandatory. Use -d OUTPUT/DIRECTORY/")
        exit(1)

if "snowflake" in sys.argv:
    v_exec = "snowflake"
else:
    if "postgresql" in sys.argv:
        v_exec = "postgresql"
    else:
        print("--> ERROR. Incorrect number of arguments. Please enter 'snowflake' or 'postgresql'")
        exit(1)

if "-t" in sys.argv:
    exec = "TRONCAL"

if "-uc" in sys.argv:
    exec = "USE_CASE"

if "-d" in sys.argv:
    try:
        sql_dir = sys.argv[sys.argv.index("-d")+1]
    except :
        raise Exception("--> ERROR. Output folder not specified." )
        exit(1)
else: 
    print("--> ERROR. An output folder must be specified. Use -d OUTPUT/DIRECTORY/")        
    exit(1)


####################################################### USE CASE METADATA ENVIRONMENT VARIABLES
LIST_EV       = { 'environments','settings','connection','environment_connections','setconnections','setconnection_connections'}
SHEET_LIST_EV = { 'environments':'EV_01_ENVIRONMENTS', 'settings':'EV_02_SETTINGS',  'connection':'EV_04_CONNECTIONS', 'environment_connections':'EV_05_ENVIRONMENT_CONNECTIONS', 'setconnections':'EV_07_SETCONNECTIONS', 'setconnection_connections':'EV_08_SETCONNECTION_CONNECTIONS'}
FILE_LIST_EV  = { 'environments':'EV_01_ENVIRONMENTS.csv', 'settings':'EV_02_SETTINGS.csv',  'connection':'EV_04_CONNECTIONS.csv', 'environment_connections':'EV_05_ENVIRONMENT_CONNECTIONS.csv',  'setconnections':'EV_07_SETCONNECTIONS.csv', 'setconnection_connections':'EV_08_SETCONNECTION_CONNECTIONS.csv'}
TABLE_LIST_EV = { 'environments':'ENVIRONMENTS_TMP', 'settings':'SETTINGS_TMP',  'connection':'CONNECTIONS_TMP', 'environment_connections':'ENVIRONMENT_CONNECTIONS_TMP',  'setconnections':'SETCONNECTIONS_TMP', 'setconnection_connections':'SETCONNECTION_CONNECTIONS_TMP'}

####################################################### USE CASE METADATA VALIDATION VARIABLES
LIST_DV       = { 'data_validation','data_validation_threshold'}
SHEET_LIST_DV = { 'data_validation':'DV_01_DATA_VALIDATION', 'data_validation_threshold':'DV_02_DATA_VALIDATION_THRESHOLD'}
FILE_LIST_DV  = { 'data_validation':'DV_01_DATA_VALIDATION.csv', 'data_validation_threshold':'DV_02_DATA_VALIDATION_THRESHOLD.csv'}
TABLE_LIST_DV = { 'data_validation':'DATA_VALIDATION_TMP', 'data_validation_threshold':'DATA_VALIDATION_THRESHOLD_TMP'}
####################################################### USE CASE METADATA DATA VARIABLES
LIST_DA       = { 'model','tenant','datasets','datasets_segments','relationships','datasets_snapshot','datasets_fields'}
SHEET_LIST_DA = { 'model':'DA_01_MODEL', 'tenant':'DA_02_TENANT', 'datasets':'DA_03_DATASETS', 'datasets_segments':'DA_04_DATASETS_SEGMENTS', 'relationships':'DA_05_RELATIONSHIPS', 'datasets_snapshot':'DA_06_DATASETS_SNAPSHOT', 'datasets_fields':'DA_07_DATASETS_FIELDS'}
FILE_LIST_DA  = { 'model':'DA_01_MODEL.csv', 'tenant':'DA_02_TENANT.csv', 'datasets':'DA_03_DATASETS.csv', 'datasets_segments':'DA_04_DATASETS_SEGMENTS.csv', 'relationships':'DA_05_RELATIONSHIPS.csv', 'datasets_snapshot':'DA_06_DATASETS_SNAPSHOT.csv', 'datasets_fields':'DA_07_DATASETS_FIELDS.csv'}
TABLE_LIST_DA = { 'model':'MODEL_TMP', 'tenant':'TENANT_TMP', 'datasets':'DATASETS_TMP', 'datasets_segments':'DATASETS_SEGMENTS_TMP', 'relationships':'RELATIONSHIPS_TMP', 'datasets_snapshot':'DATASETS_SNAPSHOT_TMP', 'datasets_fields':'DATASETS_FIELDS_TMP'}
####################################################### USE CASE METADATA PATTERN VARIABLES
LIST_PA       = { 'model_load_patterns'}
SHEET_LIST_PA = { 'model_load_patterns':'PA_01_MODEL_LOAD_PATTERNS'}
FILE_LIST_PA  = { 'model_load_patterns':'PA_01_MODEL_LOAD_PATTERNS.csv'}
TABLE_LIST_PA = { 'model_load_patterns':'MODEL_LOAD_PATTERNS_TMP'}

####################################################### TRONCAL METADATA VARIABLES
LIST_TR       = { 'area_phase','origin','dataset_field_patterns'
    , 'actions', 'threshold', 'data_validation_patterns'
    , 'data_validation_hier', 'phase', 'status'
    , 'meta_validation_dataset'}
SHEET_LIST_TR = { 'area_phase':'EV_03_AREA_PHASE','origin':'EV_06_ORIGIN','dataset_field_patterns':'PA_02_DATASET_FIELD_PATTERNS'
    , 'actions': 'PA_03_ACTIONS', 'threshold':'PA_04_THRESHOLD', 'data_validation_patterns':'PA_05_DATA_VALIDATION_PATTERNS'
    , 'data_validation_hier':'PA_06_DATA_VALIDATION_HIER', 'phase':'PA_07_PHASE', 'status':'PA_08_STATUS'
    , 'meta_validation_dataset':'DQE_01_META_VALIDATION_DATASET'}
FILE_LIST_TR  = { 'area_phase':'EV_03_AREA_PHASE.csv','origin':'EV_06_ORIGIN.csv','dataset_field_patterns':'PA_02_DATASET_FIELD_PATTERNS.csv'
    , 'actions': 'PA_03_ACTIONS.csv', 'threshold':'PA_04_THRESHOLD.csv', 'data_validation_patterns':'PA_05_DATA_VALIDATION_PATTERNS.csv'
    , 'data_validation_hier':'PA_06_DATA_VALIDATION_HIER.csv', 'phase':'PA_07_PHASE.csv', 'status':'PA_08_STATUS.csv'
    , 'meta_validation_dataset':'DQE_01_META_VALIDATION_DATASET.csv'}
TABLE_LIST_TR = { 'area_phase':'AREA_PHASE_TMP','origin':'ORIGIN_TMP','dataset_field_patterns':'DATASET_FIELD_PATTERNS_TMP'
    , 'actions':'ACTIONS_TMP', 'threshold':'THRESHOLD_TMP', 'data_validation_patterns':'DATA_VALIDATION_PATTERNS_TMP'
    , 'data_validation_hier':'DATA_VALIDATION_HIER_TMP', 'phase':'PHASE_TMP', 'status':'STATUS_TMP'
    , 'meta_validation_dataset':'TRANSLATION_METADATA_VALIDATON_DATASET_TMP'}

LIST_PL = {'metadata_validation'}
SHEET_LIST_PL = {'metadata_validation':'PA_09_META_VALIDATION_PATTERN'}
FILE_LIST_PL = {'metadata_validation':'PA_09_META_VALIDATION_PATTERN.csv'}
TABLE_LIST_PL = {'metadata_validation':'TRANSLATION_METADATA_VALIDATON_PATTERN_TMP'}


TR_LIST =  [  [LIST_TR, SHEET_LIST_TR, FILE_LIST_TR, TABLE_LIST_TR ]  ] 
PL_LIST = [ [LIST_PL, SHEET_LIST_PL, FILE_LIST_PL, TABLE_LIST_PL]]
UC_LIST =  [     [LIST_EV, SHEET_LIST_EV, FILE_LIST_EV, TABLE_LIST_EV ],
                 [LIST_DV, SHEET_LIST_DV, FILE_LIST_DV, TABLE_LIST_DV ],
                 [LIST_DA, SHEET_LIST_DA, FILE_LIST_DA, TABLE_LIST_DA ],
                 [LIST_PA, SHEET_LIST_PA, FILE_LIST_PA, TABLE_LIST_PA ]
                 ] 

if exec == "TRONCAL": 
    FINAL_LIST = TR_LIST + PL_LIST
    put_sql = 't_'+put_sql
    copy_sql= 't_'+copy_sql

if exec == "USE_CASE":
    FINAL_LIST = UC_LIST
    put_sql = 'uc_'+put_sql
    copy_sql= 'uc_'+copy_sql
    

print ("[", datetime.now() , "] [", script_def, "] Executing", exec, "process")

csv_root_dir_tmp = os.path.abspath(sql_dir)+'\\'
csv_root_dir = csv_root_dir_tmp.replace('\\','\\\\')

if v_exec == "snowflake":
    print("[", datetime.now() , "] [", script_def, "] Generation of PUT statements started")

    put_sql_txt = "--Commands generated automatically by script csv_copy_into.py -- START\n"
    put_sql_txt += "--PUT COMMAND TABLES \n"

    for list in FINAL_LIST:
        for file in list[0]:
            #print("[", datetime.now() , "]","Check file exist", sql_dir+list[2][file])
            if not os.path.isfile(sql_dir+list[2][file]):
                print("--> ERROR. File not found '", sql_dir+list[2][file], "' or does not exist")
                exit(1)
            put_sql_txt += "put 'file://"+csv_root_dir+list[2][file]+ "' " + v_stage + " OVERWRITE = TRUE;\n"
        put_sql_txt += "--PUT COMMAND TABLES \n"

    try:
        f = open(sql_dir+put_sql, "w")
    except :
        raise Exception("--> ERROR. Cannot open or write on file", sql_dir+put_sql )
        exit(1)
    
    put_sql_txt += "--Commands generated automatically by script csv_copy_into.py --END \n"

    f.write(put_sql_txt)
    f.close()
    
############################## COPY INTO

print("[", datetime.now() , "] [", script_def, "] Generation of COPY statements started")

copy_sql_txt = "--Commands generated automatically by script csv_copy_into.py -- START\n"
copy_sql_txt += "--COPY COMMAND ENVIRONMENT TABLES \n"

for list in FINAL_LIST:
    for data in list[0]:
        if v_exec == "snowflake": 
            copy_sql_txt += "TRUNCATE TABLE RAW_DATA." + list[3][data] + ";\n"
            copy_sql_txt += "COPY INTO RAW_DATA." + list[3][data] + " FROM "+ v_stage + list[2][data] +" FILE_FORMAT = ( TYPE = CSV,  FIELD_OPTIONALLY_ENCLOSED_BY = '\"' , SKIP_HEADER = 1, NULL_IF = ('[NULL]', '\\N','NULL', 'NUL') );\n"
        if v_exec == "postgresql": 
            copy_sql_txt += "TRUNCATE TABLE RAW_DATA." + list[3][data] + ";\n"
            copy_sql_txt += "\COPY RAW_DATA." + list[3][data] + " FROM '" + csv_root_dir+list[2][data] +"' WITH DELIMITER ',' CSV HEADER ENCODING 'UTF8';\n"

    copy_sql_txt += "--COPY COMMAND TABLES \n"

copy_sql_txt += "--Commands generated automatically by script csv_copy_into.py --END \n"

copy_sql = copy_sql

try:
    f = open(sql_dir+copy_sql, "w")
except :
    raise Exception("--> ERROR. Cannot open or write on file", sql_dir+copy_sql )
    exit(1)

f.write(copy_sql_txt)
f.close()
print("[", datetime.now() , "] [", script_def, "] Generation of sql process completed")

sys.exit(0)