# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Gold: Modelagem em Esquema Estrela
# MAGIC
# MAGIC Modelagem dos dados da camada Silver em um Esquema Estrela, pronto para responder às
# MAGIC perguntas de negócio definidas no objetivo do MVP (ver `README.md`):
# MAGIC
# MAGIC - `dim_titulo`: dimensão com os tipos de título do Tesouro Direto.
# MAGIC - `dim_data`: dimensão de calendário (data base da cotação).
# MAGIC - `fato_cotacao_diaria`: tabela fato com uma linha por cotação diária de um título,
# MAGIC   incluindo métricas de taxa e preço unitário e uma métrica calculada (`prazo_dias`).

# COMMAND ----------

CATALOG = "tesouro_direto"
SCHEMA_SILVER = "silver"
SCHEMA_GOLD = "gold"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA_GOLD}")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

df_silver = spark.table(f"{CATALOG}.{SCHEMA_SILVER}.preco_taxa_tesouro_direto")

# COMMAND ----------

# MAGIC %md
# MAGIC ## `dim_titulo`
# MAGIC
# MAGIC Cada tipo de título (ex.: "Tesouro Selic", "Tesouro IPCA+") recebe uma chave substituta
# MAGIC (`sk_titulo`), além de uma coluna derivada `indexador`, que classifica o título conforme o
# MAGIC indexador de rentabilidade citado em seu nome (Selic, IPCA ou Prefixado).

# COMMAND ----------

dim_titulo = (
    df_silver.select("tipo_titulo").distinct()
    .withColumn(
        "indexador",
        F.when(F.col("tipo_titulo").contains("Selic"), F.lit("Selic"))
         .when(F.col("tipo_titulo").contains("IPCA"), F.lit("IPCA"))
         .otherwise(F.lit("Prefixado")),
    )
    .withColumn("sk_titulo", F.row_number().over(Window.orderBy("tipo_titulo")))
    .select("sk_titulo", "tipo_titulo", "indexador")
)

(
    dim_titulo.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{SCHEMA_GOLD}.dim_titulo")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## `dim_data`
# MAGIC
# MAGIC Dimensão de calendário derivada das datas base distintas presentes no conjunto de dados.

# COMMAND ----------

dim_data = (
    df_silver.select(F.col("data_base").alias("data"))
    .distinct()
    .withColumn("ano", F.year("data"))
    .withColumn("mes", F.month("data"))
    .withColumn("trimestre", F.quarter("data"))
    .withColumn("dia_semana", F.date_format("data", "EEEE"))
    .withColumn("sk_data", F.date_format("data", "yyyyMMdd").cast("int"))
    .select("sk_data", "data", "ano", "mes", "trimestre", "dia_semana")
)

(
    dim_data.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{SCHEMA_GOLD}.dim_data")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## `fato_cotacao_diaria`
# MAGIC
# MAGIC Uma linha por cotação diária de um título, com as chaves substitutas das dimensões e as
# MAGIC métricas de negócio. `prazo_dias` é calculado como a diferença, em dias, entre
# MAGIC `data_vencimento` e `data_base`, útil para análises de prazo x rentabilidade.

# COMMAND ----------

fato_cotacao_diaria = (
    df_silver.alias("s")
    .join(dim_titulo.alias("t"), on="tipo_titulo", how="left")
    .withColumn("sk_data", F.date_format(F.col("s.data_base"), "yyyyMMdd").cast("int"))
    .withColumn("prazo_dias", F.datediff("data_vencimento", "data_base"))
    .select(
        "sk_titulo",
        "sk_data",
        F.col("s.data_vencimento").alias("data_vencimento"),
        "prazo_dias",
        "taxa_compra_manha",
        "taxa_venda_manha",
        "pu_compra_manha",
        "pu_venda_manha",
        "pu_base_manha",
    )
)

(
    fato_cotacao_diaria.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{SCHEMA_GOLD}.fato_cotacao_diaria")
)

# COMMAND ----------

display(spark.table(f"{CATALOG}.{SCHEMA_GOLD}.fato_cotacao_diaria").limit(10))
