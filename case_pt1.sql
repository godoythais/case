-- Verificação da quantidade de linhas da tabela e suas diferenças
SELECT 
*
,TB_LOCAL - TB_GCP AS DIFERENCA
	FROM( SELECT
    (SELECT COUNT(*) FROM application_record_local) AS  TB_LOCAL,  (SELECT COUNT(*) FROM application_record_gcp) AS TB_GCP	
);

-- Verificação da quantidade de ID distintos da tabela e suas diferenças
SELECT 
*
,TB_LOCAL - TB_GCP AS DIFERENCA_ID_DISTINTOS
	FROM( SELECT
    (SELECT COUNT(DISTINCT ID) FROM application_record_local) AS  TB_LOCAL,  (SELECT COUNT(DISTINCT ID) FROM application_record_gcp) AS TB_GCP	
);

-- Há duplicidade de ID na tabela do GCP. Se considerarmos apenas os IDs distintos, há 4051
-- IDs na tabela local que não estão na tabela do GCP.

-- Lista dos IDs que tem no local e não tem no GCP
SELECT tb_local.ID
FROM application_record_local tb_local
LEFT JOIN application_record_gcp tb_gcp
ON tb_local.ID = tb_gcp.ID
WHERE tb_gcp.ID IS NULL;

-- Lista de IDs que tem no GCP e não tem no local
SELECT tb_local.ID
FROM application_record_gcp tb_gcp
LEFT JOIN application_record_local tb_local
ON tb_local.ID = tb_gcp.ID
WHERE tb_local.ID IS NULL;

-- Conclui-se que todos os IDs que estão na tabela GCP estão na tabela local.

-- Contagem de IDs duplicados no GCP 
SELECT COUNT(1) FROM ( 
SELECT ID, COUNT(1)
FROM application_record_gcp tb_gcp
GROUP BY ID
HAVING COUNT(1) > 1 
)

-- IDs duplicados no GCP 
SELECT ID, COUNT(1)
FROM application_record_gcp tb_gcp
GROUP BY ID
HAVING COUNT(1) > 1 
ORDER BY COUNT(1) DESC

-- Análise de padrão 
SELECT tb_local.* 
FROM application_record_local tb_local
LEFT JOIN application_record_gcp tb_gcp
ON tb_local.ID = tb_gcp.ID
WHERE tb_gcp.ID IS NULL;

-- Todos os registros não presentes no GCP estão com FLAG_MOBIL = 1

SELECT tb_local.ID, tb_local.CODE_GENDER, tb_gcp.CODE_GENDER, tb_local.AMT_INCOME_TOTAL, tb_gcp.AMT_INCOME_TOTAL, tb_local.DAYS_BIRTH, tb_gcp.DAYS_BIRTH, tb_local.FLAG_WORK_PHONE, tb_gcp.FLAG_WORK_PHONE, tb_local.OCCUPATION_TYPE, tb_gcp.OCCUPATION_TYPE
--SELECT COUNT(1)
FROM application_record_local tb_local
LEFT JOIN (SELECT DISTINCT * FROM application_record_gcp) tb_gcp
ON tb_local.ID = tb_gcp.ID
--AND tb_local.CODE_GENDER  = tb_gcp.CODE_GENDER 
AND tb_local.FLAG_OWN_CAR  = tb_gcp.FLAG_OWN_CAR 
AND tb_local.FLAG_OWN_REALTY  = tb_gcp.FLAG_OWN_REALTY
AND tb_local.CNT_CHILDREN  = tb_gcp.CNT_CHILDREN 
--AND tb_local.AMT_INCOME_TOTAL = tb_gcp.AMT_INCOME_TOTAL
AND tb_local.NAME_INCOME_TYPE = tb_gcp.NAME_INCOME_TYPE
AND tb_local.NAME_EDUCATION_TYPE  = tb_gcp.NAME_EDUCATION_TYPE
AND tb_local.NAME_FAMILY_STATUS = tb_gcp.NAME_FAMILY_STATUS
AND tb_local.NAME_HOUSING_TYPE = tb_gcp.NAME_HOUSING_TYPE
--AND tb_local.DAYS_BIRTH = tb_gcp.DAYS_BIRTH
AND tb_local.DAYS_EMPLOYED = tb_gcp.DAYS_EMPLOYED
AND tb_local.FLAG_MOBIL = tb_gcp.FLAG_MOBIL
--AND tb_local.FLAG_WORK_PHONE = tb_gcp.FLAG_WORK_PHONE
AND tb_local.FLAG_PHONE = tb_gcp.FLAG_PHONE
AND tb_local.FLAG_EMAIL = tb_gcp.FLAG_EMAIL
--AND tb_local.OCCUPATION_TYPE = tb_gcp.OCCUPATION_TYPE
AND tb_local.CNT_FAM_MEMBERS = tb_gcp.CNT_FAM_MEMBERS
WHERE tb_gcp.ID IS NOT NULL;

-- Há diferença na maneira como o dado é apresentado nas seguintes colunas:
-- tb_local.CODE_GENDER, tb_gcp.CODE_GENDER, tb_local.AMT_INCOME_TOTAL, tb_gcp.AMT_INCOME_TOTAL,tb_local.DAYS_BIRTH, tb_gcp.DAYS_BIRTH, tb_local.FLAG_WORK_PHONE , 
-- tb_gcp.FLAG_WORK_PHONE, tb_local.OCCUPATION_TYPE e tb_gcp.OCCUPATION_TYPE.

-- Tratando a tabela do GCP
SELECT DISTINCT 
ID, 
CASE WHEN CODE_GENDER = 'Male' THEN 'M'
     WHEN CODE_GENDER = 'Female' THEN 'F'
ELSE NULL END AS CODE_GENDER, 
FLAG_OWN_CAR, 
FLAG_OWN_REALTY, 
CNT_CHILDREN, 
AMT_INCOME_TOTAL/100 AS AMT_INCOME_TOTAL, 
NAME_INCOME_TYPE, 
NAME_EDUCATION_TYPE, 
NAME_FAMILY_STATUS, 
NAME_HOUSING_TYPE, 
DAYS_BIRTH*(-1) AS DAYS_BIRTH, 
DAYS_EMPLOYED, 
FLAG_MOBIL, 
FLAG_WORK_PHONE, 
FLAG_PHONE, 
FLAG_EMAIL, 
CASE WHEN OCCUPATION_TYPE = 'Without Occupation' THEN ''
ELSE OCCUPATION_TYPE END AS OCCUPATION_TYPE, 
CNT_FAM_MEMBERS
FROM application_record_gcp;
