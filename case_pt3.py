# -*- coding: utf-8 -*-
import pandas as pd
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from matplotlib.ticker import PercentFormatter
from pyspark.sql import Window
from pyspark.sql.functions import when, sum, col
from pyspark.sql.functions import abs


def metrica_ks(file_name):

    spark = SparkSession.builder.appName("case").getOrCreate()
    # abrindo o arquivo csv
    df = spark.read.csv(file_name + ".csv", header=True, sep=";", encoding="latin1")
    df.show()
    # contagem do número de clientes "bom" e "mau"
    total_bom = df.filter(df['TARGET'] == 'BOM').count()
    total_mau = df.filter(df['TARGET'] == 'MAU').count()
    # ordenação do dataframe pela coluna SCORE em ordem decrescente
    df_score = df.orderBy("SCORE", ascending=False)
    window = Window.orderBy("SCORE")
    # cálculo dos acumulados de bom e mau, conforme percorre o dataframe, adiciona 1 para cada ocorrência de ambas categorias
    df_cumulative = df_score.withColumn(
        "cumulative_bom", sum(when(col("TARGET") == "BOM", 1).otherwise(0)).over(window)
    ).withColumn(
        "cumulative_mau", sum(when(col("TARGET") == "MAU", 1).otherwise(0)).over(window)
    )
    # cálculo das porcentagens acumuladas
    df_final = df_cumulative.withColumn(
        "perc_acum_bom", col("cumulative_bom") / total_bom
    ).withColumn(
        "perc_acum_mau", col("cumulative_mau") / total_mau
    )
    # cálculo de ks, diferença absoluta entre os valores acumulados de bom e mau
    df_final = df_final.withColumn(
        "diff", abs(col("perc_acum_bom") - col("perc_acum_mau"))
    )
    ks_score = df_final.agg({"diff": "max"}).collect()[0][0]
    print("KS: {}".format(ks_score))
    # plote do gráfico de ks
    df_pandas = df_final.select("perc_acum_bom", "perc_acum_mau", "score").toPandas()
    plt.plot(df_pandas['perc_acum_bom'], label='Bons', color='blue')
    plt.plot(df_pandas['perc_acum_mau'], label='Maus', color='red')
    plt.title("Curva KS: {}".format(ks_score))
    # transforma os valores do eixo de decimal para porcentagem
    plt.gca().yaxis.set_major_formatter(PercentFormatter(1))
    plt.xlim(0, 4000)
    plt.ylabel('Porcentagem Acumulada')
    plt.legend(loc='best')
    plt.show()

# Declare aqui o nome do arquivo
file_name = 'df_ks'


metrica_ks(file_name)