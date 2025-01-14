# -*- coding: utf-8 -*-

import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from matplotlib.ticker import PercentFormatter
from pyspark.sql import Window
from pyspark.sql.functions import when, sum, col
from pyspark.sql.functions import abs

def calcular_acumulados(df, window):
    """Calcula os acumulados de 'bom' e 'mau' no DataFrame."""
    df_cumulative = df.withColumn(
        "cumulative_bom", 
        sum(when(col("TARGET") == "BOM", 1).otherwise(0)).over(window)
    ).withColumn(
        "cumulative_mau", 
        sum(when(col("TARGET") == "MAU", 1).otherwise(0)).over(window)
    )
    return df_cumulative

def calcular_porcentagens_acumuladas(df, total_bom, total_mau):
    """Calcula as porcentagens acumuladas de 'bom' e 'mau'."""
    return df.withColumn(
        "perc_acum_bom", col("cumulative_bom") / total_bom
    ).withColumn(
        "perc_acum_mau", col("cumulative_mau") / total_mau
    )

def calcular_ks(df):
    """Calcula o valor de KS a partir da diferença absoluta entre as porcentagens acumuladas."""
    df = df.withColumn(
        "diff", abs(col("perc_acum_bom") - col("perc_acum_mau"))
    )
    ks_score = df.agg({"diff": "max"}).collect()[0][0]
    return ks_score

def plot_grafico(df):
    """Plota o gráfico da curva KS."""
    df_pandas = df.select("perc_acum_bom", "perc_acum_mau", "score").toPandas()

    plt.plot(df_pandas['perc_acum_bom'], label='Bons', color='blue')
    plt.plot(df_pandas['perc_acum_mau'], label='Maus', color='red')
    plt.title("Curva KS")

    # Transforma os valores do eixo de decimal para porcentagem
    plt.gca().yaxis.set_major_formatter(PercentFormatter(1))
    plt.xlim(0, 4000)
    plt.ylabel('Porcentagem Acumulada')
    plt.legend(loc='best')
    plt.show()

def metrica_ks(file_name):
    """Calcula a métrica KS a partir de um arquivo CSV e plota a curva KS."""
    spark = SparkSession.builder.appName("ks_indicator").getOrCreate()
    
    df = spark.read.csv(file_name + ".csv", header=True, sep=";", encoding="latin1")

    # Contagem do número de clientes "bom" e "mau"
    total_bom = df.filter(df['TARGET'] == 'BOM').count()
    total_mau = df.filter(df['TARGET'] == 'MAU').count()

    # Ordenação do dataframe pela coluna SCORE
    df_score = df.orderBy("SCORE", ascending=False)
    window = Window.orderBy("SCORE")

    # Calcula os acumulados
    df_cumulative = calcular_acumulados(df_score, window)

    # Calcula as porcentagens acumuladas
    df_final = calcular_porcentagens_acumuladas(df_cumulative, total_bom, total_mau)

    # Calcula o KS
    ks_score = calcular_ks(df_final)
    print("KS: {}".format(ks_score))
    
    # Plota o gráfico
    plot_grafico(df_final)

# Declare aqui o nome do arquivo
file_name = 'df_ks'

metrica_ks(file_name)