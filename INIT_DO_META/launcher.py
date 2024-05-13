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
model = "DV_TFG"
orchestation = "FALSE"
replace = "FALSE"
debug = "FALSE"
compile = False
idx = "FALSE"
type_run = "DEPLOY"


dic_models = {
    "RAW_METADATA": "DO_META_RAW",
    "GENERATOR": "DO_META_GENERATOR",
    "TEMPLATES": "DO_META_TEMPLATES",
}

do_meta = ['RAW', 'GEN', 'TEM']

dic_snw = {
    "{{varchar_max}}": "VARCHAR(16777216)",
    "{{num}}": "NUMBER",
    "{{ts_ntz}}": "TIMESTAMPNTZ",
    "TIMESTAMP(6)": "TIMESTAMPNTZ",
    "COMMIT;" : ""
}


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
debug = "FALSE"
file_remote_calls = "CALLS_" + remote_bbdd

os.environ ['DEPLOY_FOLDER'] = deploy_folder
copyFolder = deploy_folder


phase = "RAW"
con = "dv"
log = "true"
quiet = True

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
if not os.path.isdir('DEPLOY'):
    print ("[", datetime.now(), "] [", step, "/",total_steps, "]", "Creación de la carpeta DEPLOY")
    if  os.system("mkdir DEPLOY") != 0:
        print("--> Unable to create DEPLOY folder" )
        exit(1)
else:
    print ("[", datetime.now(), "] [", step, "/",total_steps, "]", "Skipped") 


if os.system("mkdir DEPLOY/" + copyFolder) != 0:
    print("--> ERROR. Unable to create temporary deploy folder " )
    exit(1)

if exists("./DEPLOY/snowsql_rt.log"):
    os.remove("./DEPLOY/snowsql_rt.log")
#FIN BLOQUE 1

#BLOQUE 2
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


#FIN BLOQUE 2

#BLOQUE 3
#DELETE DATABASE
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


#FIN BLOQUE 3

#BLOQUE 4 - Copy archivos create
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



step = step + 1

#05 - EJECUCIÓN DE LOS ARCHIVOS CREATE AND LIKE
print ("[", datetime.now(), "] [", script_def, "] [", step, "/",total_steps, "]", "Executing create tables")  

index = do_meta.index(phase.upper())
while index < len(do_meta):
    command = var_db + var_file_exec + "INIT_CREATE_" + do_meta[index].upper() + ".sql" + var_output
    if os.system(command) != 0:
        print("--> ERROR. Could not perform action" )
        exit(1)
    check_log('SNOWFLAKE')

    if do_meta[index].upper() == "NPSA" or do_meta[index].upper() == "IM":
        command = var_db + var_file_exec + "INIT_LIKE_" + do_meta[index].upper() + ".sql" + var_output
        if os.system(command) != 0:
            print("--> ERROR. Could not perform action" )
            exit(1)
        check_log('SNOWFLAKE') 
    index = index + 1

step=step+1

#6 - LEE EXCEL UC (O DEFAULT)              ALL, UC,  META-UC
# if uc == "":
#     param = "-t UC"
# else: 
#     param = "-uc " + uc + " -mo "+ model + " -t UC "

if log:
    quiet = " -log "
else:
    quiet = ""
command = "python PRE_DEPLOY/XLS_EXPORT/excel_export.py -t UC " + quiet + " -d DEPLOY/"+copyFolder+"/"
if os.system(command) != 0:
    print("--> ERROR. Could not perform action" )
    exit(1)  


step = step + 1

#7 - GENERA  PUT/COPY UC (O DEFAULT)       ALL, UC,  META-UC
print ("[", datetime.now(), "] [", script_def, "] [", step, "/",total_steps, "]", "Generation of USE CASE put/copy files started")  
if log:
    quiet = " -log "
else:
    quiet = ""    
command = "python PRE_DEPLOY/COPY_GENERATOR/generate_sql_put_copy.py -uc swnoflake " + quiet + " -d DEPLOY/"+copyFolder+"/"
if os.system(command) != 0:
    print("--> ERROR. Could not perform action" )
    exit(1)  

step = step + 1

#8 - EJECUTA PUT (O DEFAULT)       ALL, UC,  META-UC
print ("[", datetime.now(), "] [", script_def, "] [", step, "/",total_steps, "]", "Execution of USE CASE PUT statements started")     
command = var_db + var_file_exec + "uc_DV_FW_Snowflake_puts.sql " + var_output
if os.system(command) != 0:
    print("--> ERROR. Could not perform action" )
    exit(1)  
check_log('SNOWFLAKE')


step = step + 1

#9 - EJECUTA COPY UC (O DEFAULT)       ALL, UC,  META-UC
print ("[", datetime.now(), "] [", script_def, "] [", step, "/",total_steps, "]", "Execution of USE CASE COPY statements started") 
command = var_db + var_file_exec + "uc_DV_FW_copy.sql " + var_output

if os.system(command) != 0:
    print("--> ERROR. Could not perform action" )
    exit(1)  
check_log('SNOWFLAKE')



os.chdir("../DBT/projects/meta_vault")

print ("[", datetime.now(), "] [", script_def, "] [", step, "/",total_steps, "]", "dbt PSA run started")   

if log:
    command = "dbt run --profiles-dir " + profiles_dir + " --vars \"{'cod_model':'" + model + "','cod_type_run':'" + type_run + "','cod_environment':'" + environment + "','sw_debug':'" + debug.upper() + "'}\"" 
else:
    command = "dbt --quiet run --profiles-dir " + profiles_dir + " --vars \"{'cod_model':'" + model + "','cod_type_run':'" + type_run + "','cod_environment':'" + environment + "','sw_debug':'" + debug.upper() + "'}\""

if os.system(command) != 0:
    print("--> ERROR. Could not perform action" )
    exit(1)