"""
To see the help, use the next command:

    python3 excel_export.py --help

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
df_tr_dir  = "CONFIG_DEPLOY/"
df_tr_file = "DataOps_Vault_Metadata_Input_Troncal_Tables.xlsx"
df_tr_df_dir = "TRANSLATION"
df_meta_dir = "CONFIG_DEPLOY/"
df_meta_file = "DataOps_Vault_Metadata_Input_Troncal_Tables_Metadata.xlsx"
df_pl_dir  = "CONFIG_PLATFORM/"
df_pl_file = "DataOps_Vault_Metadata_Input_Platform_Tables.xlsx"
df_uc_dir  = "TRANSLATION/"
df_poc_dir = "TRANSLATION/00_POC/"
df_out_dir = "CONFIG_DEPLOY/"
script_def = "excel_export.py"

csv_dir = ""
ex_dir = {'UC/POC/'}
ex_file = {'METADATA_SCHEMA'}
exec = ""
platform = ""
####################################################### VARIABLE DEFINITION

if "--help" in sys.argv:
    help = "Documentation: \r\nUse:\r\n\tpython3 excel_export.py [-t TR|UC|PL] |[-pl SNOWFLAKE|POSTGRES]| [-uc USE_CASE_FOLDER -mo MODEL] -d DIR \r\n\r\n"
    help += "If no file or uc is given, the default artifact will be used:\n"
    help += "Options:\n"
    help += "\t-t    Exports the troncal part of the framework as csv (CONFIG_DEPLOY/).\r\n"
    help += "\t-uc   Name of the folder of the use case to be use to export the csv.\r\n"
    help += "\t-f    Exports the specified xlsx file into csv. Path to the file has to be given as input.\r\n"
    print(help)
    exit(0)
    

if "-d" in sys.argv:
    try:
        csv_dir = sys.argv[sys.argv.index("-d")+1]
    except :
        raise Exception("--> ERROR. Output folder not specified." )
else: 
    print("--> ERROR. An output folder must be specified. Use -d OUTPUT/DIRECTORY/")        
    exit(1)

if "-log" in sys.argv:
    log = True
else:
    log = False

if log:    
    print("[", datetime.now() , "] [", script_def, "] Excel export process Started")



####################################################### USE CASE METADATA ENVIRONMENT VARIABLES
LIST_EV       = { 'environments','environment_connections'}
SHEET_LIST_EV = { 'environments':'ENVIRONMENTS', 'environment_connections':'ENVIRONMENT_CONNECTIONS'}
FILE_LIST_EV  = { 'environments':'ENVIRONMENTS.csv', 'environment_connections':'ENVIRONMENT_CONNECTIONS.csv'}
TABLE_LIST_EV = { 'environments':'ENVIRONMENTS_TMP', 'environment_connections':'ENVIRONMENT_CONNECTIONS_TMP'}
####################################################### USE CASE METADATA VALIDATION VARIABLES
# LIST_DV       = { 'data_validation','data_validation_threshold'}
# SHEET_LIST_DV = { 'data_validation':'DV_01_DATA_VALIDATION', 'data_validation_threshold':'DV_02_DATA_VALIDATION_THRESHOLD'}
# FILE_LIST_DV  = { 'data_validation':'DV_01_DATA_VALIDATION.csv', 'data_validation_threshold':'DV_02_DATA_VALIDATION_THRESHOLD.csv'}
# TABLE_LIST_DV = { 'data_validation':'DATA_VALIDATION_TMP', 'data_validation_threshold':'DATA_VALIDATION_THRESHOLD_TMP'}
###################################################### USE CASE METADATA DATA VARIABLES
LIST_DA       = { 'model', 'datasets','datasets_segments','relationships','datasets_fields'}
SHEET_LIST_DA = { 'model':'MODEL', 'datasets':'DATASETS', 'datasets_segments':'DATASETS_SEGMENTS', 'relationships':'RELATIONSHIPS', 'datasets_fields':'DATASETS_FIELDS'}
FILE_LIST_DA  = { 'model':'MODEL.csv', 'datasets':'DATASETS.csv', 'datasets_segments':'DATASETS_SEGMENTS.csv', 'relationships':'RELATIONSHIPS.csv', 'datasets_fields':'DATASETS_FIELDS.csv'}
TABLE_LIST_DA = { 'model':'MODEL_TMP', 'datasets':'DATASETS_TMP', 'datasets_segments':'DATASETS_SEGMENTS_TMP', 'relationships':'RELATIONSHIPS_TMP', 'datasets_fields':'DATASETS_FIELDS_TMP'}
####################################################### USE CASE METADATA PATTERN VARIABLES
# LIST_PA       = { 'model_load_patterns'}
# SHEET_LIST_PA = { 'model_load_patterns':'PA_01_MODEL_LOAD_PATTERNS'}
# FILE_LIST_PA  = { 'model_load_patterns':'PA_01_MODEL_LOAD_PATTERNS.csv'}
# TABLE_LIST_PA = { 'model_load_patterns':'MODEL_LOAD_PATTERNS_TMP'}
####################################################### TRONCAL METADATA VARIABLES
# LIST_TR       = { 'area_phase','origin'
#     , 'actions', 'threshold', 'data_validation_patterns'
#     , 'data_validation_hier', 'phase', 'status'
#     , 'meta_validation_dataset'
#     , 'meta_validation_dataset', 'meta_sql_pattern','meta_validation_matrix'}
# SHEET_LIST_TR = { 'area_phase':'EV_03_AREA_PHASE','origin':'EV_06_ORIGIN'
#     , 'actions': 'PA_03_ACTIONS', 'threshold':'PA_04_THRESHOLD', 'data_validation_patterns':'PA_05_DATA_VALIDATION_PATTERNS'
#     , 'data_validation_hier':'PA_06_DATA_VALIDATION_HIER', 'phase':'PA_07_PHASE', 'status':'PA_08_STATUS'
#     , 'meta_validation_dataset':'DQE_01_META_VALIDATION_DATASET'
#     , 'meta_sql_pattern':'DCE_01_META_SQL_PATTERN', 'meta_validation_matrix':'DCE_02_META_VALIDATION_MATRIX'    }
# FILE_LIST_TR  = { 'area_phase':'EV_03_AREA_PHASE.csv','origin':'EV_06_ORIGIN.csv'
#     , 'actions': 'PA_03_ACTIONS.csv', 'threshold':'PA_04_THRESHOLD.csv', 'data_validation_patterns':'PA_05_DATA_VALIDATION_PATTERNS.csv'
#     , 'data_validation_hier':'PA_06_DATA_VALIDATION_HIER.csv', 'phase':'PA_07_PHASE.csv', 'status':'PA_08_STATUS.csv'
#     , 'meta_validation_dataset':'DQE_01_META_VALIDATION_DATASET.csv'
#     , 'meta_sql_pattern':'DCE_01_META_SQL_PATTERN.csv', 'meta_validation_matrix':'DCE_02_META_VALIDATION_MATRIX.csv' }
# TABLE_LIST_TR = { 'area_phase':'AREA_PHASE_TMP','origin':'ORIGIN_TMP'
#     , 'actions':'ACTIONS_TMP', 'threshold':'THRESHOLD_TMP', 'data_validation_patterns':'DATA_VALIDATION_PATTERNS_TMP'
#     , 'data_validation_hier':'DATA_VALIDATION_HIER_TMP', 'phase':'PHASE_TMP', 'status':'STATUS_TMP'
#     , 'meta_validation_dataset':'TRANSLATION_METADATA_VALIDATION_DATASET_TMP'
#     , 'meta_sql_pattern':'META_SQL_PATTERN_TMP', 'meta_validation_matrix':'META_VALIDATION_MATRIX_TMP'    }


####################################################### TRONCAL DATSET FIELD PATTERNS
# LIST_TR_DF = {'dataset_field_patterns'}
# SHEET_LIST_TR_DF = {'dataset_field_patterns':'PA_02_DATASET_FIELD_PATTERNS'}
# FILE_LIST_TR_DF = {'dataset_field_patterns':'PA_02_DATASET_FIELD_PATTERNS.csv'}
# TABLE_LIST_TR_DF = {'dataset_field_patterns':'DATASET_FIELD_PATTERNS_TMP'}

# ####################################################### TRONCAL META DATSETS
# LIST_TR_META = {'meta_datasets'}
# SHEET_LIST_TR_META = {'meta_datasets':'META_01_DATASETS'}
# FILE_LIST_TR_META = {'meta_datasets':'META_01_DATASETS.csv'}
# TABLE_LIST_TR_META = {'meta_datasets':'META_DATASETS_TMP'}



# ####################################################### PLATFORM METADATA VARIABLES
# LIST_PL = {'metadata_validation'}
# SHEET_LIST_PL = {'metadata_validation':'PA_09_META_VALIDATION_PATTERN'}
# FILE_LIST_PL = {'metadata_validation':'PA_09_META_VALIDATION_PATTERN.csv'}
# TABLE_LIST_PL = {'metadata_validation':'TRANSLATION_METADATA_VALIDATION_PATTERN_TMP'}


UC_LIST =  [     [LIST_EV, SHEET_LIST_EV, FILE_LIST_EV, TABLE_LIST_EV ],
                #  [LIST_DV, SHEET_LIST_DV, FILE_LIST_DV, TABLE_LIST_DV ],
                 [LIST_DA, SHEET_LIST_DA, FILE_LIST_DA, TABLE_LIST_DA ],
                #  [LIST_PA, SHEET_LIST_PA, FILE_LIST_PA, TABLE_LIST_PA ]
                 ] 


FINAL_LIST = {"UC":UC_LIST}

if log:
    print ("[", datetime.now() , "] [", script_def, "] Executing", exec, "process")
    print ("[", datetime.now() , "] [", script_def, "] Directory to use:" ,ex_dir)
    print ("[", datetime.now() , "] [", script_def, "] Excel File to use:",ex_file)

    print("[", datetime.now() , "] [", script_def, "] Extracting excel sheets from (", ex_file, ") to csv files into folder:", csv_dir  )

for key,parentlist in FINAL_LIST.items():
    for list in parentlist:
        for env in list[0]:
            if log:
                print("[", datetime.now() , "] [", script_def, "] Reading sheet " + list[1][env]+ " Started")
            try:
                read_file = pd.read_excel ('../UC/POC/METADATA_SCHEMA.xlsx', sheet_name=list[1][env], keep_default_na=False, na_values='' )
            except FileNotFoundError:
                raise Exception("--> ERROR. File not found: "+ ex_dir + ex_file )
            except ValueError:
                raise Exception("--> ERROR. Unable to open excel sheet: " + ex_file )  
            except PermissionError:
                raise Exception("--> ERROR. Unable to open excel file: " + ex_file + ". Please close the excel file and re-run the script." )  
            except :
                raise Exception("--> ERROR. Unable to process file: " + ex_file )            

            read_file = read_file.dropna(how='all')
            
            if log:
                print("[", datetime.now() , "] [", script_def, "] Writing data to file " + csv_dir + list[2][env] + " Started")
            
            try:
                read_file.to_csv (csv_dir + list[2][env]+'_tmp', index = None, header=True, quotechar = '"', quoting=csv.QUOTE_ALL, doublequote=True, date_format='%d/%m/%Y' )
            except FileNotFoundError:
                raise Exception("--> ERROR. File not found: "+ ex_dir + ex_file+'_tmp' )
            except :
                raise Exception("--> ERROR. Unable to write fo file : " + list[2][env]+'_tmp' )
                exit(1)        

            line = ""

            try:
                f = open(csv_dir + list[2][env]+'_tmp', "r")
                for x in f:
                    line += x.replace('"[NULL]"','')
            except:
                raise Exception("--> ERROR. Unable to open fo file : " + list[2][env]+'_tmp' )        

            try: 
                f = open(csv_dir + list[2][env], "w")
                f.write(line)
                f.close()
            except:
                raise Exception("--> ERROR. Unable to write fo file : " + list[2][env] )        

            os.remove(csv_dir + list[2][env]+'_tmp')

if log:
    print("[", datetime.now() , "] [", script_def, "] Excel export process Completed")

sys.exit(0)