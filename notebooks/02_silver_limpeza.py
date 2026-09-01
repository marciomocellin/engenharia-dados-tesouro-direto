# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Silver: Limpeza, tipagem e padronização
# MAGIC
# MAGIC Nesta etapa o dado bruto da camada Bronze é limpo e padronizado:
# MAGIC
# MAGIC - Conversão de tipos: datas (`dd/MM/yyyy` -> `date`) e valores numéricos (vírgula -> ponto, `string` -> `double`).
# MAGIC - Remoção de duplicatas exatas.
# MAGIC - Tratamento de nulos/linhas incompletas.
# MAGIC - Padronização de texto da coluna `tipo_titulo` (trim + capitalização consistente).
# MAGIC - Renomeação de colunas para `snake_case`, facilitando o consumo em SQL.

# COMMAND ----------

CATALOG = "tesouro_direto"
SCHEMA_BRONZE = "bronze"
SCHEMA_SILVER = "silver"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA_SILVER}")

# COMMAND ----------

from pyspark.sql import functions as F

df_bronze = spark.table(f"{CATALOG}.{SCHEMA_BRONZE}.preco_taxa_tesouro_direto")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1) Renomeação e conversão de tipos

# COMMAND ----------

def para_double(coluna):
    """Converte string com vírgula decimal (padrão BR) para double."""
    return F.regexp_replace(F.col(coluna), ",", ".").cast("double")


df_silver = (
    df_bronze
    .withColumnRenamed("Tipo Titulo", "tipo_titulo")
    .withColumn("data_vencimento", F.to_date(F.col("Data Vencimento"), "dd/MM/yyyy"))
    .withColumn("data_base", F.to_date(F.col("Data Base"), "dd/MM/yyyy"))
    .withColumn("taxa_compra_manha", para_double("Taxa Compra Manha"))
    .withColumn("taxa_venda_manha", para_double("Taxa Venda Manha"))
    .withColumn("pu_compra_manha", para_double("PU Compra Manha"))
    .withColumn("pu_venda_manha", para_double("PU Venda Manha"))
    .withColumn("pu_base_manha", para_double("PU Base Manha"))
    .withColumn("tipo_titulo", F.trim(F.col("tipo_titulo")))
    .select(
        "tipo_titulo",
        "data_vencimento",
        "data_base",
        "taxa_compra_manha",
        "taxa_venda_manha",
        "pu_compra_manha",
        "pu_venda_manha",
        "pu_base_manha",
        "_ingestion_timestamp",
        "_source_file",
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2) Remoção de duplicatas
# MAGIC
# MAGIC A combinação `(tipo_titulo, data_vencimento, data_base)` identifica unicamente uma
# MAGIC cotação diária de um título. Registros duplicados nessa chave são removidos, mantendo a
# MAGIC primeira ocorrência.

# COMMAND ----------

linhas_antes = df_silver.count()

df_silver = df_silver.dropDuplicates(["tipo_titulo", "data_vencimento", "data_base"])

linhas_depois = df_silver.count()
print(f"Duplicatas removidas: {linhas_antes - linhas_depois}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3) Tratamento de nulos e inconsistências
# MAGIC
# MAGIC Registros sem `tipo_titulo`, `data_base` ou `data_vencimento` não têm utilidade
# MAGIC analítica e são descartados. Taxas/PUs nulos ou negativos (impossíveis para este domínio)
# MAGIC também são descartados, pois indicam erro de carga na fonte.

# COMMAND ----------

linhas_antes = df_silver.count()

df_silver = df_silver.filter(
    F.col("tipo_titulo").isNotNull()
    & F.col("data_base").isNotNull()
    & F.col("data_vencimento").isNotNull()
    & F.col("pu_base_manha").isNotNull()
    & (F.col("pu_base_manha") > 0)
)

linhas_depois = df_silver.count()
print(f"Linhas descartadas por qualidade: {linhas_antes - linhas_depois}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gravação da tabela Silver (Delta)

# COMMAND ----------

(
    df_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{SCHEMA_SILVER}.preco_taxa_tesouro_direto")
)

display(spark.table(f"{CATALOG}.{SCHEMA_SILVER}.preco_taxa_tesouro_direto").limit(10))
