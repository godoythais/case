# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
import requests
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import sum as sum_, col

def download_csv(url, file_name):    
    """Baixa o arquivo CSV da URL e salva no arquivo especificado."""
    response = requests.get(url)
    if response.status_code == 200:
        with open(file_name, 'wb') as file:
            file.write(response.content)
    else:
        print("Erro ao baixar o CSV:", response.status_code)

def tratamento_colunas(df):
    """Realiza o tratamento de colunas do DataFrame."""
    df = df.withColumn('CO_VIA', df['CO_VIA'].cast(IntegerType()))
    df = df.withColumn('CO_PAIS', df['CO_PAIS'].cast(IntegerType()))
    df = df.withColumn('CO_NCM', df['CO_NCM'].cast(IntegerType()))
    df = df.withColumn('VL_FOB', col('VL_FOB').cast('float'))
    df = df.withColumn('KG_LIQUIDO', df['KG_LIQUIDO'].cast(IntegerType()))
    return df

def filtro_dados(df):
    """Filtra os dados de acordo com os critérios fornecidos."""
    return df[
        (df['CO_PAIS'] == '275') &
        (df['CO_VIA'] == '1') &
        (df['CO_NCM'] == '33030010') &
        (df['SG_UF_NCM'] == 'SP')
    ]

def calculo_media(df):
    """Calcula a média do valor do produto por kg."""
    df_soma = df.agg(
        sum_(col('VL_FOB')).alias('VALOR_TOTAL_SOMA'),
        sum_(col('KG_LIQUIDO')).alias('KG_LIQUIDO_SOMA')
    )
    resultado = df_soma.collect()
    valor_total_soma = resultado[0]['VALOR_TOTAL_SOMA']
    kg_liquido_soma = resultado[0]['KG_LIQUIDO_SOMA']

    if kg_liquido_soma > 0:
        media_valor_produto = valor_total_soma / kg_liquido_soma
        print("Média do valor do produto em dólares por kg no ano: ${}\n\n\n".format(media_valor_produto))
    else:
        print("A quantidade total vendida é zero, média não pode ser calculada.")

def analise_csv(url,file_name):
    """Função principal para realizar a análise do arquivo CSV."""
    spark = SparkSession.builder.appName("value_dollar").getOrCreate()
    #Download dos arquivos
    download_csv(url, file_name)

    # Leitura do arquivo csv
    df = spark.read.csv(file_name, header=True, sep=";", encoding="latin1")

    # Tratamento das colunas
    df = tratamento_colunas(df)

    # Filtragem dos dados
    df_filtered = filtro_dados(df)

    # Exibir os dados filtrados
    print("Dados filtrados:")
    df_filtered.show()

    # Calcular a soma e a média
    calculo_media(df_filtered)

def main():
    """Função principal para rodar a análise em múltiplos arquivos."""

    urls = ['https://balanca.economia.gov.br/balanca/bd/comexstat-bd/ncm/IMP_2023.csv', 'https://balanca.economia.gov.br/balanca/bd/comexstat-bd/ncm/IMP_2024.csv']
    file_name = ['IMP_2023.csv', 'IMP_2024.csv']

    for url, file_name in zip(urls, file_name):
        analise_csv(url, file_name)

if __name__ == "__main__":
    main()