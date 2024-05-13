"""
To see the help, use the next command:

    python excel_export.py --help

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
df_pl_dir  = "CONFIG_PLATFORM/"
df_pl_file = "DataOps_Vault_Metadata_Input_Platform_Tables.xlsx"
df_uc_dir  = "TRANSLATION/01_CLIENTS/"
df_poc_dir = "TRANSLATION/00_POC/00_LAST/"
df_out_dir = "CONFIG_DEPLOY/"
script_def = "excel_export.py"

csv_dir = ""
ex_dir = {}
ex_file = {}
exec = ""
platform = ""
####################################################### VARIABLE DEFINITION

if "--help" in sys.argv:
    help = "Documentation: \r\nUse:\r\n\tpython excel_export.py [-t TR|UC|PL] |[-pl SNOWFLAKE|POSTGRES]| [-uc USE_CASE_FOLDER -mo MODEL] -d DIR \r\n\r\n"
    help += "If no file or uc is given, the default artifact will be used:\n"
    help += "Options:\n"
    help += "\t-t    Exports the troncal part of the framework as csv (CONFIG_DEPLOY/).\r\n"
    help += "\t-uc   Name of the folder of the use case to be use to export the csv.\r\n"
    help += "\t-f    Exports the specified xlsx file into csv. Path to the file has to be given as input.\r\n"
    print(help)
    exit(0)
    
print("[", datetime.now() , "] [", script_def, "] Excel export process Started")

#VALIDATE INPUT PARAMETERS
if ("-uc" in sys.argv and "-mo" not in sys.argv) or ("-uc" not in sys.argv and "-mo" in sys.argv):
        print("--> ERROR. Both USE CASE and MODEL must be specified")        
        exit(1)

if ("-t" in sys.argv and ("-mo" in sys.argv or "-uc" not in sys.argv and "-mo" in sys.argv)):
        print("--> ERROR. Cannot use TRONCAL combined with USE CASE or MODEL")        
        exit(1)

if ("-t" not in sys.argv and "-pl" not in sys.argv and "-uc" not in sys.argv and "-mo" not in sys.argv): 
        print("--> ERROR. It is mandatory to specify the platform or the use case")        
        exit(1)        

if ("-d" not in sys.argv ):
        print("--> ERROR. Output directory is mandatory. Use -d OUTPUT/DIRECTORY/")
        exit(1)

if "-pl" in sys.argv: 
    try:
        platform = sys.argv[sys.argv.index("-pl")+1] 
        if platform in ["SNOWFLAKE", "POSTGRESQL"]: 
            df_pl_dir = df_pl_dir + platform + "/"
        else:
            raise Exception ("--> ERROR. The Platform " + platform + "is invalid. The allowed values are SNOWFLAKE and POSTGRESQL" )
            exit(1)     
    except: 
        raise Exception ("--> ERROR. Could not assign Platform." )
        exit(1)  

if "-t" in sys.argv:
    try:
        t_run = sys.argv[sys.argv.index("-t")+1] 
        if t_run == "TR":
            if platform in ["SNOWFLAKE", "POSTGRESQL"]: 
                ex_dir = {"TR":df_tr_dir,"PL":df_pl_dir}
                ex_file = {"TR":df_tr_file,"PL":df_pl_file} 
            else:
                print ("--> ERROR. The Platform " + platform + "is invalid. The allowed values are SNOWFLAKE and POSTGRESQL" )
                exit(1)               
        if t_run == "UC":
            ex_dir = {"UC":df_poc_dir}
            for k,v in ex_dir.items():
                ex_file = {k:find('*.xlsx', v)}      
        exec = "TRONCAL"
    except :
        raise Exception("--> ERROR. Could not assign Troncal file." )
        exit(1)


if "-uc" in sys.argv and "-mo" in sys.argv:
    try:
        client = sys.argv[sys.argv.index("-uc")+1] + '/'
        ex_dir = {"UC":df_uc_dir + client + sys.argv[sys.argv.index("-mo")+1] + '/'}
        for k,v in ex_dir.items():
            ex_file = {k:find('*.xlsx', v)}    
        exec = "USE_CASE"
    except IndexError:
        raise Exception("--> ERROR. USE CASE name not specified. Please specify one." )

if "-f" in sys.argv:
    try:
        tmp_ex_file = sys.argv[sys.argv.index("-f")+1]
        ex_dir  = {"F":os.path.dirname(tmp_ex_file)+'/'}
        ex_file = {"F":os.path.basename(tmp_ex_file)}
    except IndexError:
        raise Exception("--> ERROR. File name not specified. Please specify one." )

if "-d" in sys.argv:
    try:
        csv_dir = sys.argv[sys.argv.index("-d")+1]
    except :
        raise Exception("--> ERROR. Output folder not specified." )
else: 
    print("--> ERROR. An output folder must be specified. Use -d OUTPUT/DIRECTORY/")        
    exit(1)


if len(ex_dir) == 0:
    if platform in ["SNOWFLAKE", "POSTGRESQL"]: 
        ex_dir = {"TR":df_tr_dir,"PL":df_pl_dir}
        ex_file = {"TR":df_tr_file,"PL":df_pl_file} 
    else:
        print ("--> ERROR. The Platform " + platform + "is invalid. The allowed values are SNOWFLAKE and POSTGRESQL" )
        exit(1)  
                
if len(ex_file)==0:
    ex_file = {"TR":find('*.xlsx', ex_dir["TR"]), "PL":find('*.xlsx', ex_dir["PL"])}

for dir in ex_dir.values():
    if not os.path.isdir(dir):
        print("--> ERROR. Folder not found '", dir, "' or does not exist")
        exit(1)

for key, file in ex_file.items():
    if not os.path.isfile(ex_dir[key]+file):
        print("--> ERROR. File not found '", ex_dir[key]+file, "' or does not exist")
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

####################################################### PLATFORM METADATA VARIABLES
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

if exec == "TRONCAL" and t_run == "TR": 
    FINAL_LIST = {"TR":TR_LIST,  "PL":PL_LIST}

if exec == "USE_CASE" or (exec == "TRONCAL" and t_run == "UC"):
    FINAL_LIST = {"UC":UC_LIST}

print ("[", datetime.now() , "] [", script_def, "] Executing", exec, "process")
print ("[", datetime.now() , "] [", script_def, "] Directory to use:" ,ex_dir)
print ("[", datetime.now() , "] [", script_def, "] Excel File to use:",ex_file)

print("[", datetime.now() , "] [", script_def, "] Extracting excel sheets from (", ex_file, ") to csv files into folder:", csv_dir  )

for key,parentlist in FINAL_LIST.items():
    for list in parentlist:
        for env in list[0]:
            print("[", datetime.now() , "] [", script_def, "] Reading sheet " + list[1][env]+ " Started")
            try:
                read_file = pd.read_excel (ex_dir[key] + ex_file[key], sheet_name=list[1][env], keep_default_na=False, na_values='' )
            except FileNotFoundError:
                raise Exception("--> ERROR. File not found: "+ ex_dir + ex_file )
            except ValueError:
                raise Exception("--> ERROR. Unable to open excel sheet: " + ex_file )  
            except PermissionError:
                raise Exception("--> ERROR. Unable to open excel file: " + ex_file + ". Please close the excel file and re-run the script." )  
            except :
                raise Exception("--> ERROR. Unable to process file: " + ex_file )            

            read_file = read_file.dropna(how='all')

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

print("[", datetime.now() , "] [", script_def, "] Excel export process Completed")

sys.exit(0)