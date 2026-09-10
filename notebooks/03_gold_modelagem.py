# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
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

spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA_GOLD}.dim_titulo") # Caso queira limpar a tabela Bronze

# COMMAND ----------

# DBTITLE 1,Célula 5
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA_GOLD}.dim_titulo (
        sk_titulo BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1) COMMENT 'Chave substituta da dimensão título',
        tipo_titulo STRING COMMENT 'Nome completo do tipo de título do Tesouro Direto',
        indexador STRING COMMENT 'Indexador de rentabilidade: Selic, IPCA, Prefixado ou IGPM',
        PRIMARY KEY (sk_titulo, tipo_titulo)
    )
    USING DELTA
    COMMENT 'Dimensão de tipos de título do Tesouro Direto'
    PARTITIONED BY (tipo_titulo)
""")

# COMMAND ----------

existing_df = spark.table(f"{CATALOG}.{SCHEMA_GOLD}.dim_titulo").select("tipo_titulo").distinct()

dim_titulo = (
    df_silver.select("tipo_titulo").distinct().join(
    existing_df,
    on=["Tipo_Titulo"],
    how="left_anti"
)
    .withColumn(
        "indexador",
        F.when(F.col("tipo_titulo").contains("Selic"), F.lit("Selic"))
         .when(F.col("tipo_titulo").contains("IPCA"), F.lit("IPCA"))
         .when(F.col("tipo_titulo").contains("Prefixado"), F.lit("Prefixado"))
         .when(F.col("tipo_titulo").contains("IGPM"), F.lit("IGPM"))
         .otherwise(F.lit("IPCA")),
    )
)
dim_titulo.display()

# COMMAND ----------

# DBTITLE 1,Célula 6
dim_titulo.createOrReplaceTempView("tmp_dim_titulo")
spark.sql(f"""
    INSERT INTO {CATALOG}.{SCHEMA_GOLD}.dim_titulo (tipo_titulo, indexador)
    SELECT * FROM tmp_dim_titulo
""")

# COMMAND ----------

display(spark.table(f"{CATALOG}.{SCHEMA_GOLD}.dim_titulo").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## `dim_data`
# MAGIC
# MAGIC Dimensão de calendário derivada das datas base distintas presentes no conjunto de dados.

# COMMAND ----------

# DBTITLE 1,Célula 10
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA_GOLD}.dim_data (
        sk_data INT COMMENT 'Chave substituta da dimensão data (formato yyyyMMdd)',
        data DATE COMMENT 'Data base da cotação',
        ano INT COMMENT 'Ano da data base',
        mes INT COMMENT 'Mês da data base (1-12)',
        trimestre INT COMMENT 'Trimestre da data base (1-4)',
        dia_semana STRING COMMENT 'Dia da semana por extenso',
        PRIMARY KEY (sk_data)
    )
    USING DELTA
    PARTITIONED BY (ano)
    COMMENT 'Dimensão de calendário com atributos temporais'
""")

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

dim_data.createOrReplaceTempView("tmp_dim_data")
spark.sql(f"""
    INSERT OVERWRITE TABLE {CATALOG}.{SCHEMA_GOLD}.dim_data
    SELECT * FROM tmp_dim_data
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## `fato_cotacao_diaria`
# MAGIC
# MAGIC Uma linha por cotação diária de um título, com as chaves substitutas das dimensões e as
# MAGIC métricas de negócio. `prazo_dias` é calculado como a diferença, em dias, entre
# MAGIC `data_vencimento` e `data_base`, útil para análises de prazo x rentabilidade.

# COMMAND ----------

# DBTITLE 1,Célula 12
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA_GOLD}.fato_cotacao_diaria (
        sk_titulo INT COMMENT 'Chave estrangeira para dim_titulo',
        sk_data INT COMMENT 'Chave estrangeira para dim_data',
        data_vencimento DATE COMMENT 'Data de vencimento do título',
        prazo_dias INT COMMENT 'Prazo em dias entre data_base e data_vencimento',
        taxa_compra_manha DECIMAL(10,6) COMMENT 'Taxa de compra na posição da manhã',
        taxa_venda_manha DECIMAL(10,6) COMMENT 'Taxa de venda na posição da manhã',
        pu_compra_manha DECIMAL(18,6) COMMENT 'Preço unitário de compra na manhã',
        pu_venda_manha DECIMAL(18,6) COMMENT 'Preço unitário de venda na manhã',
        pu_base_manha DECIMAL(18,6) COMMENT 'Preço unitário base na manhã',
        PRIMARY KEY (sk_titulo, sk_data, data_vencimento)
    )
    USING DELTA
    PARTITIONED BY (sk_data)
    COMMENT 'Tabela fato com cotações diárias dos títulos do Tesouro Direto'
""")

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

fato_cotacao_diaria.createOrReplaceTempView("tmp_fato_cotacao_diaria")
spark.sql(f"""
    INSERT OVERWRITE TABLE {CATALOG}.{SCHEMA_GOLD}.fato_cotacao_diaria
    SELECT * FROM tmp_fato_cotacao_diaria
""")

# COMMAND ----------

display(spark.table(f"{CATALOG}.{SCHEMA_GOLD}.fato_cotacao_diaria").limit(10))