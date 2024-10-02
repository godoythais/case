# -*- coding: utf-8 -*-
import os
import pandas as pd
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import requests
from pyspark.sql.functions import col, regexp_replace, sum as spark_sum
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col, round, format_number
from pyspark.sql.functions import concat, lit
from pyspark.sql.functions import sum as sum_, col

def analise_csv(url,file_name):
    spark = SparkSession.builder.appName("case").getOrCreate()
    # desativar a verificação do certificado, o site do governo está com problema na verificação do SSL
    try:
        response = requests.get(url, verify=False)
        if response.status_code == 200:
            with open(file_name, 'wb') as f:
                f.write(response.content)
        else:
            print("Erro ao baixar o CSV:", response.status_code)
    except requests.exceptions.SSLError as e:
        print("Erro de SSL:", e)
    # leitura do arquivo csv
    df = spark.read.csv(file_name, header=True, sep=";", encoding="latin1")
    df.head()
    # tratamento das colunas
    df = df.withColumn('CO_VIA', df['CO_VIA'].cast(IntegerType()))
    df = df.withColumn('CO_PAIS', df['CO_PAIS'].cast(IntegerType()))
    df = df.withColumn('CO_NCM', df['CO_NCM'].cast(IntegerType()))
    df = df.withColumn('VL_FOB', col('VL_FOB').cast('float'))
    df = df.withColumn('KG_LIQUIDO', df['KG_LIQUIDO'].cast(IntegerType()))
    df.show()
    # filtro de acordo com os requisitos país França, via navio, produto 33030010 e negociação no estado de SP
    df_filtered = df[
        (df['CO_PAIS'] == '275') &
        (df['CO_VIA'] == '1') &
        (df['CO_NCM'] == '33030010') &
        (df['SG_UF_NCM'] == 'SP') ]
    print(df_filtered)
    df_filtered.show()
    # soma dos valores das colunas
    df_soma = df_filtered.agg(
        sum_(col('VL_FOB')).alias('VALOR_TOTAL_SOMA'),
        sum_(col('KG_LIQUIDO')).alias('KG_LIQUIDO_SOMA'))
    df_soma.show()
    resultado = df_soma.collect()
    valor_total_soma = resultado[0]['VALOR_TOTAL_SOMA']
    kg_liquido_soma = resultado[0]['KG_LIQUIDO_SOMA']

    if kg_liquido_soma > 0:
        media_valor_produto = valor_total_soma / kg_liquido_soma
        print("Média do valor do produto em dólares por kg no ano:{}".format(media_valor_produto))
    else:
        print("A quantidade total vendida é zero, média não pode ser calculada.")

urls = ['https://balanca.economia.gov.br/balanca/bd/comexstat-bd/ncm/IMP_2023.csv', 'https://balanca.economia.gov.br/balanca/bd/comexstat-bd/ncm/IMP_2024.csv']
file_name = ['IMP_2023.csv', 'IMP_2024.csv']

for url, file_name in zip(urls, file_name):
    analise_csv(url, file_name)

