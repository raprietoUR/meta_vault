

WITH ORIG_BBDD AS (
    SELECT DIM.COD_DATASET, CON.COD_PATH_1||'.'||CON.COD_PATH_2||'.' AS BBDD_PATH
    FROM edw.gen_phase.elements DIM 
        JOIN edw.gen_phase.setconnection_connections SETCON ON DIM.COD_SETCONNECTION = SETCON.COD_SETCONNECTION
        JOIN edw.gen_phase.connections CON ON SETCON.COD_CONNECTION = CON.COD_CONNECTION
        JOIN edw.gen_phase.environment_connections ECON ON CON.COD_CONNECTION = ECON.COD_CONNECTION
        WHERE ECON.COD_ENVIRONMENT = 'PRO'
)
SELECT DV_ELEMENT.COD_DATASET,
    (SELECT 'MD5('||LISTAGG(C2.COD_DATASET_FIELD, ',') WITHIN GROUP (ORDER BY C2.ID_ORDER)||') AS HUB_ID'                                                               
				FROM edw.gen_phase.elements C2 
                WHERE DV_ELEMENT.COD_DATASET= C2.COD_DATASET 
                    AND C2.COD_TYPE = 'PK'
                ) LIST_TARGET_PK,
    (SELECT LISTAGG(C2.COD_COLUMN_NAME_TARGET, ',') WITHIN GROUP (ORDER BY C2.ID_ORDER)                                                               
				FROM edw.gen_phase.elements C2 
                WHERE DV_ELEMENT.COD_DATASET= C2.COD_DATASET 
                    AND C2.COD_TYPE = 'BK'
                ) LIST_AKA,
    (SELECT LISTAGG( '''' || C2.COD_DATASET_FIELD||''' : ' ||  C2.COD_COLUMN_NAME_TARGET , ',') WITHIN GROUP (ORDER BY ID_ORDER)                                                                     
				FROM edw.gen_phase.elements C2 WHERE DV_ELEMENT.COD_DATASET= C2.COD_DATASET
                AND C2.COD_TYPE='PK'
                ) LIST_TARGET_ID,
    (SELECT LISTAGG(C2.COD_DATASET_FIELD ||' AS ' ||C2.COD_COLUMN_NAME_TARGET, ',') WITHIN GROUP (ORDER BY C2.ID_ORDER)                          
			FROM edw.gen_phase.elements C2 WHERE DV_ELEMENT.COD_DATASET= C2.COD_DATASET                                                                                                                                                                            
			AND C2.COD_TYPE LIKE '%BK%') LIST_TARGET_BK,
    'SELECT HUB_ID AS '||DV_ELEMENT.COD_DATASET_NAME||', '||LIST_AKA
        ||' FROM (SELECT '||LIST_TARGET_PK||', '||LIST_TARGET_BK||' FROM '||ODB.BBDD_PATH||DV_ELEMENT.COD_DATASET_ORIGIN||')'
    --'CREATE TABLE '||COD_TYPE_DATASET||'_'||COD_DATASET_NAME||' AS ('||query||')' 
FROM edw.gen_phase.elements DV_ELEMENT 
    JOIN ORIG_BBDD ODB ON DV_ELEMENT.COD_DATASET = ODB.COD_DATASET
WHERE COD_TYPE_DATASET = 'HUB'