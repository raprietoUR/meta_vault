{{ config(
    schema='tem_generator',
    alias='hubs_templates',
    materialized='incremental'
) }}

--depends_on: {{ ref('TEM_INIT') }}


WITH ORIG_BBDD AS (
    SELECT DIM.COD_DATASET, CON.COD_PATH_1||'.'||CON.COD_PATH_2||'.' AS BBDD_PATH, CON.COD_PATH_1 BBDD
    FROM {{ ref('GEN_LOAD_ELEMENTS') }} DIM 
        JOIN {{ ref('GEN_LOAD_SETCONNECTION_CONN') }} SETCON ON DIM.COD_SETCONNECTION = SETCON.COD_SETCONNECTION
        JOIN {{ ref('GEN_LOAD_CONNECTIONS') }} CON ON SETCON.COD_CONNECTION = CON.COD_CONNECTION
        JOIN {{ ref('GEN_LOAD_ENVIRONMENT_CONN') }} ECON ON CON.COD_CONNECTION = ECON.COD_CONNECTION
        WHERE ECON.COD_ENVIRONMENT = 'PRO'
)
SELECT DISTINCT DV_ELEMENT.COD_DATASET,
    (SELECT 'MD5('||LISTAGG(C2.COD_DATASET_FIELD, ',') WITHIN GROUP (ORDER BY C2.ID_ORDER)||') AS HUB_ID'                                                               
				FROM {{ ref('GEN_LOAD_ELEMENTS') }} C2 
                WHERE DV_ELEMENT.COD_DATASET= C2.COD_DATASET 
                    AND C2.COD_TYPE = 'PK'
                ) LIST_TARGET_PK,
    (SELECT LISTAGG(C2.COD_COLUMN_NAME_TARGET, ',') WITHIN GROUP (ORDER BY C2.ID_ORDER)                                                               
				FROM {{ ref('GEN_LOAD_ELEMENTS') }} C2 
                WHERE DV_ELEMENT.COD_DATASET= C2.COD_DATASET 
                    AND C2.COD_TYPE = 'BK'
                ) LIST_AKA,
    (SELECT LISTAGG( '''' || C2.COD_DATASET_FIELD||''' : ' ||  C2.COD_COLUMN_NAME_TARGET , ',') WITHIN GROUP (ORDER BY ID_ORDER)                                                                     
				FROM {{ ref('GEN_LOAD_ELEMENTS') }} C2 WHERE DV_ELEMENT.COD_DATASET= C2.COD_DATASET
                AND C2.COD_TYPE='PK'
                ) LIST_TARGET_ID,
    (SELECT LISTAGG(C2.COD_DATASET_FIELD ||' AS ' ||C2.COD_COLUMN_NAME_TARGET, ',') WITHIN GROUP (ORDER BY C2.ID_ORDER)                          
			FROM {{ ref('GEN_LOAD_ELEMENTS') }} C2 WHERE DV_ELEMENT.COD_DATASET= C2.COD_DATASET                                                                                                                                                                            
			AND C2.COD_TYPE LIKE '%BK%') LIST_TARGET_BK,
    (SELECT LISTAGG('TMP.'||C2.COD_COLUMN_NAME_TARGET, ',') WITHIN GROUP (ORDER BY C2.COD_TYPE DESC, C2.ID_ORDER)                          
			FROM {{ ref('GEN_LOAD_ELEMENTS') }} C2 WHERE DV_ELEMENT.COD_DATASET= C2.COD_DATASET) LIST_AKA_TMP,       
    'SELECT HUB_ID AS HUB_ID_'||DV_ELEMENT.COD_DATASET_NAME||', '||LIST_AKA
        ||' FROM (SELECT '||LIST_TARGET_PK||', '||LIST_TARGET_BK||' FROM '||ODB.BBDD_PATH||DV_ELEMENT.COD_DATASET_ORIGIN||')' CREATE_QUERY,
    'CREATE TABLE '||ODB.BBDD||'.DC_DATA_RDV.'||COD_TYPE_DATASET||'_'||COD_DATASET_NAME||' AS ('||CREATE_QUERY||')'  CREATE_IFNOT,
    'SELECT CASE WHEN COUNT(1)>0 THEN ''Y'' ELSE ''N'' END SW_EXISTE FROM EDW.INFORMATION_SCHEMA.TABLES WHERE TABLE_CATALOG || ''.'' || TABLE_SCHEMA || ''.'' || TABLE_NAME = '''||ODB.BBDD_PATH||DV_ELEMENT.COD_DATASET_NAME||''';' CHECK_IF_EXISTS,
    'SELECT DISTINCT '||LIST_AKA_TMP||' FROM ('||CREATE_QUERY||') TMP LEFT JOIN '||ODB.BBDD||'.DC_DATA_RDV.'||COD_TYPE_DATASET||'_'||COD_DATASET_NAME 
    ||' HUB ON TMP.HUB_ID_'||DV_ELEMENT.COD_DATASET_NAME||' = HUB.'||DV_ELEMENT.COD_DATASET_NAME INSERT_QUERY,
    ' WHERE HUB.'||DV_ELEMENT.COD_DATASET_NAME||' IS NULL' INSERT_WHERE_CLAUSE,
    'INSERT INTO '||ODB.BBDD||'.DC_DATA_RDV.'||COD_TYPE_DATASET||'_'||COD_DATASET_NAME||' '||INSERT_QUERY||INSERT_WHERE_CLAUSE INSERT_IF
FROM {{ ref('GEN_LOAD_ELEMENTS') }} DV_ELEMENT 
    JOIN ORIG_BBDD ODB ON DV_ELEMENT.COD_DATASET = ODB.COD_DATASET
WHERE COD_TYPE_DATASET = 'HUB'
