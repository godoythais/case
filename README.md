### Projeto de análise de dados
Este repositório contém scripts desenvolvidos para resolver diferentes problemas relacionados à análise de dados e métricas de negócios. Cada script está documentado com detalhes sobre sua funcionalidade, objetivo e metodologia. Abaixo estão os principais problemas abordados:

# database_comparison
Script desenvolvido para verificar inconsistências entre dois bancos de dados, permitindo a identificação de diferenças e alinhamento das informações.

# behavior_score
Consulta implementada no BigQuery para analisar o comportamento de revendedores. Este script avalia:
- A quantidade de dias de atraso no pagamento de boletos;
- O faturamento total de cada revendedor.

# ks_indicator
Função que calcula a métrica KS (Kolmogorov-Smirnov), amplamente utilizada para avaliar a eficácia de modelos de classificação. O cálculo compara as distribuições acumuladas de duas classes, identificando o maior desvio entre elas.

# value_dollar
Script responsável por realizar o download de tabelas governamentais com dados sobre preços de produtos importados.
Processar e filtrar os dados para obter informações específicas:
- País: França.
- Modalidade de transporte: navio.
- Produto: código 33030010.
- Estado de negociação: SP.
Calcular a média do valor em dólar do produto nos anos de 2023 e 2024.

## Tecnologias utilizadas
- Python e SQL;
- Postgres para criação do database;
- BigQuery.

## Pré requisitos
- Python 3.9+;
- Criação de um database em algum SGBD para execução dos scripts SQL;
- Bibliotecas como Matplotlib e Pyspark.

## Arquitetura
|--- application_record_gcp.csv # Dados usados para execução dos scripts database_comparison

|--- application_record_local.csv # Dados usados para execução dos scripts database_comparison

|--- behavior_score.sql # Arquivo SQL para análise de dias de atraso no pagamento de revendedores

|--- database_comparison.ipynb # Arquivo notebook com comparação dos dados de application_record

|--- database_comparison.sql # Arquivo SQL com comparação dos dados de application_record

|--- df_ks.csv # Dados usados em ks_indicator.py

|--- ks_indicator.py # Função em python para cálculo da métrica KS usada pelo mercado de crédito

|--- tb_revendedor.csv # Dados usados em behavior_score

|--- tb_titulos.csv # Dados usados em behavior_score

|--- value_dollar.py # Script python que calcula a média do dólar para determinado produto nos anos de 2023 e 2024

## Instalação
- Clone o repositório;
- Garanta que as bibliotecas necessárias estão instaladas;
- Criação do database em um SGBD usados os arquivos application_record_gcp, application_record_local, tb_titulos e tb_revendedor

