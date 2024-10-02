-- EXEMPLO DE CHAMADA DA PROCEDURE:
-- CALL teste.case_2('2022-09-20')
-- As tabelas e a procedure foram criadas em sintaxe do Big Query dentro do data set 'teste'.

-- data_parametro é a referência de data da qual se espera o resultado da análise
CREATE OR REPLACE PROCEDURE teste.case_pt2(data_parametro DATE)

BEGIN

-- tabela de dias de atraso por revendedor
CREATE TEMP TABLE tb_dias_atraso AS (
SELECT
re.nm_revendedor, IF(dt_pagamento IS NULL,DATE_DIFF(data_parametro,dt_vencimento,DAY ),MAX(DATE_DIFF(dt_pagamento,dt_vencimento,DAY ))) AS dias_atraso
FROM teste.tb_revendedor re
LEFT JOIN teste.tb_titulos ti 
    ON re.id_revendedor = ti.id_revendedor
GROUP BY nm_revendedor,dt_pagamento,dt_vencimento);

-- máximo dias de atraso em 1 mês
SELECT
nm_revendedor, MAX(dias_atraso) AS MAX_DIAS_ATRASO_1M
FROM tb_dias_atraso
WHERE dias_atraso <= 30
GROUP BY ALL;

-- máximo dias de atraso em 3 meses
SELECT
nm_revendedor, MAX(dias_atraso) AS MAX_DIAS_ATRASO_3M
FROM tb_dias_atraso
WHERE dias_atraso <= 90
GROUP BY ALL;

-- total faturado em 3 meses (round usado para arredondamento)
SELECT
re.nm_revendedor, round(SUM(ti.vlr_pedido),2) AS vlr_total
FROM teste.tb_revendedor re
LEFT JOIN teste.tb_titulos ti 
ON re.id_revendedor = ti.id_revendedor
WHERE ti.dt_vencimento >= DATE_SUB(data_parametro, INTERVAL 3 MONTH)
GROUP BY ALL;

-- quantidade de títulos em boleto a prazo em 3 meses
SELECT
nm_revendedor, COUNT(ti.id_revendedor) AS qtde_titulo
FROM teste.tb_revendedor re
LEFT JOIN teste.tb_titulos ti
ON re.id_revendedor = ti.id_revendedor
WHERE forma_pagamento = 'Boleto a Prazo'
AND dt_pagamento >= DATE_SUB(data_parametro, INTERVAL 3 MONTH)
GROUP BY nm_revendedor;

END