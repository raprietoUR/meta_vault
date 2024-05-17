import sys
import time
from datetime import datetime
import os
import platform
from multiprocessing.pool import Pool
import multiprocessing
import shutil
#import psycopg2
import snowflake.connector
from os.path import exists


environment = "PRO"
model = "EDW_TFG"
orchestation = "FALSE"
replace = "FALSE"
debug = "FALSE"
compile = False
idx = "FALSE"
type_run = "DEPLOY"


dic_models = {
    "RAW": "RAW_DATA",
    "GEN": "GEN_PHASE",
    "TEM": "TEM_GENERATOR",
}

do_meta = ['RAW', 'GEN', 'TEM']




debug = "FALSE"
phase = "RAW"
con = "dv"
log = "true"
quiet = True


script_def = sys.argv[0]

if "--help" in sys.argv:
    help = "Usage: \r\n\tpython RUN_DV_FW_MAIN.py -remote IND -c CONNECTION -p PLATFORM -m MODE -e ENV -t TENTANT [-ph PHASE] -uc USECASE -mo MODEL [-o] [-r] [-d] \r\n\r\n"
    help += "Mandatory parameters:\r\n"
    help += "\t\t-remote,                  Executes DBT or HIB py.\r\n"
    help += "\t\tN,                        Executes the Artifact\r\n"
    help += "\t\tY,                        Executes the Hybrid mode\r\n"
    help += "\t-p,                         Platform to deploy. There are 2 options:\r\n"
    help += "\t\tSNOWFLAKE,                Deploys in snowflake\r\n"
    help += "\t\tPOSTGRESQL,               Deploys in postgresql (cannot be used for hybrid mode) \r\n"
    help += "\t-m,                         Mode to deploy. There are 4 options:\r\n"
    help += "\t\tALL,                      Deletes and creates the entire database, loads all troncal and use case metadata into Snowflake and runs the artifact starting at PSA. It also launch EXPORT and IMPORT if -remote Y (hybrid mode)\r\n"
    help += "\t\tMETA,                     Truncates and inserts the TRONCAL metadata in Snowflake and executes the artifact starting in -ph.\r\n"
    help += "\t\tUC,                       Truncates and inserts the USE CASE metadata in Snowflake and executes the artifact starting in -ph.\r\n"
    help += "\t\tORCH,                     Executes the orchestrator (this only works in Snowflake).)\r\n"    
    help += "\t\tMETA-UC,                  Executes the metadata from the UC.\r\n"
    help += "\t\tDEPLOY,                   MAIN LOAD.\r\n"    
    help += "\t\tCSV,                      Exports the csv files.\r\n"
    help += "\t\tEXPORT,                   Launch Hibrid EXPORT local data and Hibrid IMPORT into destination. (Only useful for hybrid mode).\r\n"
    help += "\t-t,                         Tenant to be used. This is required if a custom use case is used.\r\n"
    help += "\t-uc,                        Name of the USE CASE to load.\r\n"
    help += "\t\t-e,                       Name of the environment to be deployed.\r\n"
    help += "\t\t-mo,                      Model to be used.\r\n"
    help += "Optional parameters:\r\n"
    help += "-ph,                          Indicates the initial phase the artifact will start from. This parameter is compulsory when -m in COMP, META or UC. There are 5 options:\r\n"
    help += "\t\tPSA,                      Executes PSA, PSA-Validations, NPSA, RDV, BDV and IM.\r\n"
    help += "\t\tNPSA,                     Executes NPSA, RDV, BDV and IM.\r\n"
    help += "\t\tRDV,                      Executes RDV, BDV and IM.\r\n"
    help += "\t\tBDV,                      Executes BDV and IM.\r\n"
    help += "\t\tIM,                       Executes IM only.\r\n"
    help += "\t-co,                        Compiles the main Store Procedure script and executes it, loads the TRONCAL and USE CASE metadata and executes the artifact starting at -ph. Used mainly for development.\r\n"    
    help += "\t-d,                         Executes de deployment and/or orchestration with the full debug mode activated.\r\n"
    help += "\t-o,                         Executes the orchestration of the artifact.\r\n"
    help += "\t-r,                         Executes the replacement of the tables and proc.\r\n"
    help += "\t-i,                         Truncates the destination tables (used only on HYBRID mode).\r\n"  
    help += "\t-log,                       Writes all log statements into terminal.\r\n"  
    help += "\t\r\n"
    print(help)
    exit(1)
start_time = time.time() #STARTING PROCESS
print("[", datetime.now() , "] RUN_DV_FW_MAIN.py Process Started")

  

if "-c" in sys.argv:
    try:
        con = sys.argv[sys.argv.index("-c")+1]
    except:
        raise Exception("--> ERROR. Connection must be specified." )
        exit(1)
else:
    print("--> ERROR: Connection (-c) must be specified.")
    exit(1)

if "-m" in sys.argv:
    try: 
        mod = sys.argv[sys.argv.index("-m")+1]
    except IndexError:
        raise Exception("--> ERROR. Use MODE must be specified when using -m." )
        exit(1)
else:
    print("--> ERROR: Run MODE (-m) must be specified.")
    exit(1)    

if mod.upper() not in ["ALL","TR-UC","TR","UC", "GENERATE","TEMPLATES", "DEPLOY", "CSV"]:   
    print("--> ERROR: Mode (-m) must be one of the following: ALL, META, UC, TR-UC, DEPLOY, CSV, GENERATE")
    exit(1)

if "-ph" in sys.argv:
    try:
        phase = sys.argv[sys.argv.index("-ph")+1]
    except:
        raise Exception("--> ERROR. Connection must be specified." )
        exit(1)
else:
    if mod.upper() not in ["ALL", "CSV"]:
        print("--> ERROR: Phase (-ph) must be specified if mode is not ALL, ORCH or CSV")
        exit(1)

if mod.upper() in ["TR", "UC", "TR-UC"] and phase.upper() not in ["RAW_DATA"]:
    print("--> ERROR: Phase (-ph) must be PSA if mode is META, UC or META-UC")
    exit(1)

if mod.upper() not in ["ALL", "CSV"]:
    if phase.upper() not in ["RAW","GEN","TEM"]:   
        print("--> ERROR: Phase (-ph) must be one of the following: RAW, GEN, TEM")
        exit(1)

if mod.upper() in ["ALL"]:
    phase = "RAW"  
    compile = True

if "-uc" in sys.argv:
    try:
        uc = sys.argv[sys.argv.index("-uc")+1]
    except IndexError:
        raise Exception("--> ERROR. Use Case must be specified when using -uc." )
        exit(1)
else:
    uc = ""

if "-log" in sys.argv:
    log = True
else:
    log = False


with open('../connection_configuration.txt') as archivo_conf:
    lineas = archivo_conf.readlines()
connection_vars={}
for linea in lineas:
    partes_separadas = linea.strip().split('=')

    if len(partes_separadas) == 2:
        nomb_variable, variable = partes_separadas
        connection_vars[nomb_variable] = variable
        os.environ[nomb_variable] = variable

remote_bbdd=os.environ['SNOW_DATABASE_REMOTE']
remote_schema=os.environ['SNOW_SCHEMA_REMOTE']
std_acc=os.environ['SNOW_ACCOUNT']
std_user=os.environ['SNOW_USER']
std_password=os.environ['SNOW_PASSWORD']

connection_vars['SNOW_DATABASE']=os.environ['SNOW_DATABASE']
connection_vars['SNOW_ROLE']=os.environ['SNOW_ROLE']
connection_vars['SNOW_WAREHOUSE']=os.environ['SNOW_WAREHOUSE']
connection_vars['SNOW_SCHEMA']=os.environ['SNOW_SCHEMA']


mycommand = 'sed -e "s/\[ACCOUNT]/' + connection_vars['SNOW_ACCOUNT'] + '/g"' \
      ' -e "s/\[USERNAME\]/' + connection_vars['SNOW_USER'] + '/g"' \
      ' -e "s/\[PASSWORD\]/' + connection_vars['SNOW_PASSWORD'] + '/g"'  \
      ' -e "s/\[DATABASE\]/' + connection_vars['SNOW_DATABASE'] + '/g"' \
      ' -e "s/\[ROLE\]/' + connection_vars['SNOW_ROLE'] + '/g"' \
      ' -e "s/\[WAREHOUSE\]/' + connection_vars['SNOW_WAREHOUSE'] + '/g"' \
      ' -e "s/\[SCHEMA\]/' + connection_vars['SNOW_SCHEMA'] + '/g"'    \
      ' -e "s/\[ACCOUNT_REMOTE\]/' + connection_vars['SNOW_ACCOUNT_REMOTE'] + '/g"' \
      ' -e "s/\[USERNAME_REMOTE\]/'+ connection_vars['SNOW_USER_REMOTE'] +'/g"' \
      ' -e "s/\[PASSWORD_REMOTE\]/'+ connection_vars['SNOW_PASSWORD_REMOTE'] +'/g"'  \
      ' -e "s/\[DATABASE_REMOTE\]/'+ connection_vars['SNOW_DATABASE_REMOTE'] +'/g"' \
      ' -e "s/\[ROLE_REMOTE\]/' + connection_vars['SNOW_ROLE_REMOTE'] +'/g"' \
      ' -e "s/\[WAREHOUSE_REMOTE\]/' + connection_vars['SNOW_WAREHOUSE_REMOTE'] +'/g"' \
      ' -e "s/\[SCHEMA_REMOTE\]/'+ connection_vars['SNOW_SCHEMA_REMOTE'] +'/g"'    \
      ' /dv_gen/config_template > ~/.snowsql/config'

if os.system(mycommand) != 0:
        print("--> ERROR. Could not perform action on " + mycommand)
        exit(1)


now = datetime.now()
current_time = now.strftime("%Y-%m-%d_%H%M%S")
deploy_folder = current_time + "_DEPLOY"
script_def = sys.argv[0]

file_remote_calls = "CALLS_" + remote_bbdd

os.environ ['DEPLOY_FOLDER'] = deploy_folder
copyFolder = deploy_folder




step = 1
total_steps = 10

conn_snowflake = snowflake.connector.connect(user=std_user,
                                                password=std_password,
                                                account=std_acc,
                                                warehouse="EDW_WH",
                                                database="EDW")



def check_log(platform):
    if platform == "SNOWFLAKE":
        try:
            with open('DEPLOY/snowsql_rt.log', 'r', encoding='utf-8') as file:
                data = file.readlines()
            if data:
                shutil.move(r'DEPLOY/snowsql_rt.log',r'DEPLOY/' + deploy_folder + r'/snowsql_rt.log')
                #shutil.move(r'DEPLOY/snowsql_rt.log_bootstrap',r'DEPLOY/' + deploy_folder + r'/snowsql_rt.log_bootstrap')
                print("--> ERROR. Check the Snowflake log file for more details. (snowsql_rt.log) " )
                exit(1)
            return 0
        except FileNotFoundError:
            raise Exception("The file snowsql_rt.log was not found. Make sure you have performed all the steps of the README file in the snowsql setup.")


query_BBDD = "SELECT DISTINCT CN.COD_PATH_1 as BBDD FROM RAW_DATA.DATASETS DS INNER JOIN RAW_DATA.SETCONNECTION_CONNECTIONS SC ON DS.COD_SETCONNECTION = SC.COD_SETCONNECTION  AND DS.ID_MODEL_RUN = SC.ID_MODEL_RUN INNER JOIN RAW_DATA.ENVIRONMENT_CONNECTIONS EC ON SC.COD_CONNECTION = EC.COD_CONNECTION  AND SC.ID_MODEL_RUN = EC.ID_MODEL_RUN INNER JOIN RAW_DATA.CONNECTIONS CN ON EC.COD_CONNECTION = CN.COD_CONNECTION   AND EC.ID_MODEL_RUN = CN.ID_MODEL_RUN WHERE DS.DT_LOAD IN (SELECT MAX(DT_LOAD) FROM RAW_DATA.DATASETS WHERE COD_MODEL='" + model + "') AND EC.COD_ENVIRONMENT = '" + environment + "' ;"

df_uc_dir  = "../UC/"
deploy_folder = os.environ.get ("DEPLOY_FOLDER")
STATIC_SQL = ["INIT_DROP","INIT_SCRIPT_DATABASE","INIT_SCRIPT_TABLES"]

profiles_dir = '../../profiles/snowflake' 
creates_dir = "./INIT_SCRIPT_TABLES/"

var_db = "snowsql -c "+ con 
var_db_init_snow = "snowsql -c dv_init "
if log:
    var_output = " -o friendly=false -o insecure_mode=True "
else:
    var_output = " -o quiet=true -o friendly=false -o insecure_mode=True "
    
var_file_exec = " -f DEPLOY/"+deploy_folder+"/"


#BLOQUE 1
if mod.upper() not in ('TEMPLATES'):
    if not os.path.isdir('DEPLOY'):
        print ("[", datetime.now(), "] [", step, "/",total_steps, "]", "Creación de la carpeta DEPLOY")
        if  os.system("mkdir DEPLOY") != 0:
            print("--> Unable to create DEPLOY folder" )
            exit(1)


    if os.system("mkdir DEPLOY/" + copyFolder) != 0:
        print("--> ERROR. Unable to create temporary deploy folder " )
        exit(1)

    if exists("./DEPLOY/snowsql_rt.log"):
        os.remove("./DEPLOY/snowsql_rt.log")

else:
    print ("[", datetime.now(), "] [", step, "/",total_steps, "]", "Skipped") 
#FIN BLOQUE 1

#BLOQUE 2
if mod.upper() in ('ALL'):
    original = "./INIT_DROP.sql"
    target = "DEPLOY/"+copyFolder+"/INIT_DROP.sql"
    try:
        shutil.copyfile(original, target)
    except:
        raise Exception("--> ERROR. No se ha podido copiar el archivo INIT_DROP.sql en la carpeta DEPLOY" )
        exit(1)

    original = "./INIT_SCRIPT_DATABASE.sql"
    target = "DEPLOY/"+copyFolder+"/INIT_SCRIPT_DATABASE.sql"
    try:
        shutil.copyfile(original, target)
    except:
        raise Exception("--> ERROR. No se ha podido copiar el archivo INIT_SCRIPT_DATABASE en la carpeta DEPLOY" )
        exit(1)
else:
    print ("[", datetime.now(), "] [", step, "/",total_steps, "]", "Skipped") 


#FIN BLOQUE 2

#BLOQUE 3
#DELETE DATABASE
if mod.upper() in ('ALL'):
    command = var_db_init_snow + var_file_exec + "INIT_DROP.sql " + var_output
    if os.system(command) != 0:
            print("--> ERROR. Could not perform action" )
            exit(1)  
    check_log('SNOWFLAKE')

    #CREATE DATABASE
    command = var_db_init_snow + var_file_exec + "INIT_SCRIPT_DATABASE.sql " + var_output  
    if os.system(command) != 0:
            print("--> ERROR. Could not perform action" )
            exit(1)  
    check_log('SNOWFLAKE')
else:
    print ("[", datetime.now(), "] [", step, "/",total_steps, "]", "Skipped") 


#FIN BLOQUE 3

#BLOQUE 4 - Copy archivos create
if mod.upper() in ('ALL'):
    print ("[", datetime.now(), "] [", script_def, "] [", step, "/",total_steps, "]", "Copying template create files into deploy folder")  

    index = do_meta.index(phase.upper())
    while index < len(do_meta):
        original = creates_dir + "INIT_CREATE_"+ do_meta[index].upper() + ".sql"
        target = "DEPLOY/"+copyFolder+"/INIT_CREATE_" + do_meta[index].upper() + ".sql"
        try:
            shutil.copyfile(original, target)
        except:
            raise Exception("--> ERROR. Unable to copy file into DEPLOY folder" )
            exit(1)

        if do_meta[index].upper() == "NPSA" or do_meta[index].upper() == "IM":
            original = creates_dir + "INIT_LIKE_"+ do_meta[index].upper() + ".sql"
            target = "DEPLOY/"+copyFolder+"/INIT_LIKE_" + do_meta[index].upper() + ".sql"
            try:
                shutil.copyfile(original, target)
            except:
                raise Exception("--> ERROR. Unable to copy file into DEPLOY folder" )
                exit(1)
        index = index + 1
else:
    print ("[", datetime.now(), "] [", step, "/",total_steps, "]", "Skipped") 



step = step + 1

#05 - EJECUCIÓN DE LOS ARCHIVOS CREATE AND LIKE
if mod.upper() in ('ALL'):
    print ("[", datetime.now(), "] [", script_def, "] [", step, "/",total_steps, "]", "Executing create tables")  

    index = do_meta.index(phase.upper())
    while index < len(do_meta):
        command = var_db + var_file_exec + "INIT_CREATE_" + do_meta[index].upper() + ".sql" + var_output
        print(command)
        if os.system(command) != 0:
            print("--> ERROR. Could not perform action" )
            exit(1)
        check_log('SNOWFLAKE')
        index = index + 1
else:
    print ("[", datetime.now(), "] [", step, "/",total_steps, "]", "Skipped") 
step=step+1

#6 - LEE EXCEL UC (O DEFAULT)              ALL, UC,  META-UC
if mod.upper() in ('ALL', 'TR-UC', 'UC', 'CSV'):
    if uc == "":
        param = "-t UC"
    else: 
        param = "-uc " + uc + " -mo "+ model + " -t UC "

    if log:
        quiet = " -log "
    else:
        quiet = ""
    command = "python PRE_DEPLOY/XLS_EXPORT/excel_export.py -t UC " + quiet + " -d DEPLOY/"+copyFolder+"/"
    if os.system(command) != 0:
        print("--> ERROR. Could not perform action" )
        exit(1)  
else:
    print ("[", datetime.now(), "] [", step, "/",total_steps, "]", "Skipped") 


step = step + 1

#7 - GENERA  PUT/COPY UC (O DEFAULT)       ALL, UC,  META-UC
if mod.upper() in ('ALL', 'TR-UC', 'UC', 'CSV'):
    print ("[", datetime.now(), "] [", script_def, "] [", step, "/",total_steps, "]", "Generation of USE CASE put/copy files started")  
    if log:
        quiet = " -log "
    else:
        quiet = ""    
    command = "python PRE_DEPLOY/COPY_GENERATOR/generate_sql_put_copy.py -uc swnoflake " + quiet + " -d DEPLOY/"+copyFolder+"/"
    if os.system(command) != 0:
        print("--> ERROR. Could not perform action" )
        exit(1)  
else:
    print ("[", datetime.now(), "] [", step, "/",total_steps, "]", "Skipped") 

step = step + 1

#8 - EJECUTA PUT (O DEFAULT)       ALL, UC,  META-UC
if mod.upper() in ('ALL', 'TR-UC', 'UC', 'CSV'):
    print ("[", datetime.now(), "] [", script_def, "] [", step, "/",total_steps, "]", "Execution of USE CASE PUT statements started")     
    command = var_db + var_file_exec + "uc_DV_FW_Snowflake_puts.sql " + var_output
    if os.system(command) != 0:
        print("--> ERROR. Could not perform action" )
        exit(1)  
    check_log('SNOWFLAKE')
else:
    print ("[", datetime.now(), "] [", step, "/",total_steps, "]", "Skipped") 


step = step + 1

#9 - EJECUTA COPY UC (O DEFAULT)       ALL, UC,  META-UC
if mod.upper() in ('ALL', 'TR-UC', 'UC', 'CSV'):
    print ("[", datetime.now(), "] [", script_def, "] [", step, "/",total_steps, "]", "Execution of USE CASE COPY statements started") 
    command = var_db + var_file_exec + "uc_DV_FW_copy.sql " + var_output

    if os.system(command) != 0:
        print("--> ERROR. Could not perform action" )
        exit(1)  
    check_log('SNOWFLAKE')
else:
    print ("[", datetime.now(), "] [", step, "/",total_steps, "]", "Skipped") 



def creacionColumns():
    #05 - CREACION DEL COLUMNS.CSV -  -m ALL, UC,  META-UC
    print ("[", datetime.now(), "] [", script_def, "] [", step, "/",total_steps, "]", "Generation of COLUMNS csv file started")
    bbdds = []

    command = var_db + " -q \"" + query_BBDD + "\" -o output_format=csv -o header=false -o timing=false -o friendly=false > DEPLOY/" + copyFolder + "/BBDD.csv"

    if os.system(command) != 0:
        print("--> ERROR. Could not perform action. Please verify that at least, one full execution has been performed in Snowflake" )
        exit(1)
    check_log('SNOWFLAKE')
    
    with open("DEPLOY/" + copyFolder + "/BBDD.csv",'r') as file:
        temp = file.read().splitlines()
        for line in temp:
            bbdds.append(line.replace('"', ''))

    
    for index, bbdd in enumerate(bbdds):
        if index == 0:
            command = "snowsql -c " + con + " -d " + bbdd + " -q \"SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, COLUMN_DEFAULT, IS_NULLABLE, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, CHARACTER_OCTET_LENGTH, NUMERIC_PRECISION, NUMERIC_PRECISION_RADIX, NUMERIC_SCALE, DATETIME_PRECISION, INTERVAL_TYPE, INTERVAL_PRECISION, CHARACTER_SET_CATALOG, CHARACTER_SET_SCHEMA, CHARACTER_SET_NAME, COLLATION_CATALOG, COLLATION_SCHEMA, COLLATION_NAME, DOMAIN_CATALOG, DOMAIN_SCHEMA, DOMAIN_NAME, UDT_CATALOG, UDT_SCHEMA, UDT_NAME, SCOPE_CATALOG, SCOPE_SCHEMA, SCOPE_NAME, MAXIMUM_CARDINALITY, DTD_IDENTIFIER, IS_SELF_REFERENCING, IS_IDENTITY, IDENTITY_GENERATION, IDENTITY_START, IDENTITY_INCREMENT, IDENTITY_MAXIMUM, IDENTITY_MINIMUM, IDENTITY_CYCLE, NULL AS COMMENT FROM INFORMATION_SCHEMA.\"COLUMNS\"\" -o output_format=csv -o timing=false -o friendly=false > DEPLOY/" + copyFolder + "/COLUMNS.csv"
        else:
            command = "snowsql -c " + con + " -d " + bbdd + " -q \"SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, COLUMN_DEFAULT, IS_NULLABLE, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, CHARACTER_OCTET_LENGTH, NUMERIC_PRECISION, NUMERIC_PRECISION_RADIX, NUMERIC_SCALE, DATETIME_PRECISION, INTERVAL_TYPE, INTERVAL_PRECISION, CHARACTER_SET_CATALOG, CHARACTER_SET_SCHEMA, CHARACTER_SET_NAME, COLLATION_CATALOG, COLLATION_SCHEMA, COLLATION_NAME, DOMAIN_CATALOG, DOMAIN_SCHEMA, DOMAIN_NAME, UDT_CATALOG, UDT_SCHEMA, UDT_NAME, SCOPE_CATALOG, SCOPE_SCHEMA, SCOPE_NAME, MAXIMUM_CARDINALITY, DTD_IDENTIFIER, IS_SELF_REFERENCING, IS_IDENTITY, IDENTITY_GENERATION, IDENTITY_START, IDENTITY_INCREMENT, IDENTITY_MAXIMUM, IDENTITY_MINIMUM, IDENTITY_CYCLE, NULL AS COMMENT FROM INFORMATION_SCHEMA.\"COLUMNS\"\" -o header=false -o output_format=csv -o timing=false -o friendly=false >> DEPLOY/" + copyFolder + "/COLUMNS.csv"

        if os.system(command) != 0:
            print("--> ERROR. Could not perform action" )
            exit(1)
    
    data = ""


def ejecutaColumns():
    #05 - EJECUTA COLUMNS  -   -m ALL, UC,  META-UC
    print("[", datetime.now(), "] [", script_def, "] [", step, "/",total_steps, "]", "Launching COLUMNS csv file started")

    command = var_db + " -q \"TRUNCATE TABLE RAW_DATA.DATASETS_FIELDS_COLUMNS_TMP\"" + var_output
    
    if os.system(command) != 0:
        print("--> ERROR. Could not perform action" )
        exit(1)
    

    command = var_db+ " -q \"put 'file:///dv_gen/INIT_DO_META/DEPLOY/" + copyFolder +"/COLUMNS.csv' @~/UPLOADS/ OVERWRITE = TRUE;\""  + var_output
    if os.system(command) != 0:
        print("--> ERROR. Could not perform action" )
        exit(1)
    
    command = var_db + " -q \"COPY INTO RAW_DATA.DATASETS_FIELDS_COLUMNS_TMP FROM @~/UPLOADS/COLUMNS.csv FILE_FORMAT = ( TYPE = CSV,  FIELD_OPTIONALLY_ENCLOSED_BY = '\\\"' , SKIP_HEADER = 1, NULL_IF = ('[NULL]', 'NULL', 'NUL' , ''));\""  + var_output
    if os.system(command) != 0:
        print("--> ERROR. Could not perform action" + command )
        exit(1)





if mod.upper() in ('ALL', 'TR-UC', 'UC', 'DEPLOY'):
    os.chdir("../DBT/projects/meta_vault")

    print ("[", datetime.now(), "] [", script_def, "] [", step, "/",total_steps, "]", "dbt RAW run started")   

    for item in dic_models:
        os.environ['SNOW_SCHEMA'] = dic_models[item.upper()]

        

        if log:
            command = "dbt run --models " + dic_models[item.upper()] + " --profiles-dir " + profiles_dir + " --vars \"{'cod_model':'" + model + "','cod_type_run':'" + type_run + "','cod_environment':'" + environment + "','sw_debug':'" + debug.upper() + "'}\"" 
        else:
            command = "dbt --quiet run --models " + dic_models[item.upper()] + " --profiles-dir " + profiles_dir + " --vars \"{'cod_model':'" + model + "','cod_type_run':'" + type_run + "','cod_environment':'" + environment + "','sw_debug':'" + debug.upper() + "'}\"" 
        
        if log:
            print(command)

        if os.system(command) != 0:
            print("--> ERROR. Could not perform action" )
            exit(1)

        if item.upper() == "RAW":
            os.chdir("../../../INIT_DO_META")
            step = step + 1
            creacionColumns()
            step = step + 1
            ejecutaColumns()
            os.chdir("../DBT/projects/meta_vault")

            step = step + 1
            print ("[", datetime.now(), "] [", script_def, "] [", step, "/",total_steps, "]", "dbt RAW_LOAD_COLUMNS run started")

            if log:
                command = "dbt run --models RAW_LOAD_COLUMNS --profiles-dir " + profiles_dir + " --vars \"{'cod_model':'" + model + "','cod_type_run':'" + type_run + "','cod_environment':'" + environment + "','sw_debug':'" + debug.upper() + "'}\"" 
            else:
                command = "dbt --quiet run --models RAW_LOAD_COLUMNS --profiles-dir " + profiles_dir + " --vars \"{'cod_model':'" + model + "','cod_type_run':'" + type_run + "','cod_environment':'" + environment + "','sw_debug':'" + debug.upper() + "'}\"" 
            
            if os.system(command) != 0:
                print("--> ERROR. Could not perform action" )
                exit(1)
else:
    print ("[", datetime.now(), "] [", step, "/",total_steps, "]", "Skipped") 