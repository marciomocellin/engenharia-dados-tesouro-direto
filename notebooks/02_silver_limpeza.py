# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
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

df_bronze.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1) Renomeação e conversão de tipos

# COMMAND ----------

from pyspark.sql.types import DecimalType

def para_decimal(coluna):
    """Converte string com vírgula decimal (padrão BR) para decimal(10,2)."""
    return F.regexp_replace(F.col(coluna), ",", ".").cast(DecimalType(10, 2))


df_silver = (
    df_bronze
    .withColumnRenamed("Tipo_Titulo", "tipo_titulo")
    .withColumn("data_vencimento", F.to_date(F.col("Data_Vencimento"), "dd/MM/yyyy"))
    .withColumn("data_base", F.to_date(F.col("Data_Base"), "dd/MM/yyyy"))
    .withColumn("taxa_compra_manha", para_decimal("Taxa_Compra_Manha"))
    .withColumn("taxa_venda_manha", para_decimal("Taxa_Venda_Manha"))
    .withColumn("pu_compra_manha", para_decimal("PU_Compra_Manha"))
    .withColumn("pu_venda_manha", para_decimal("PU_Venda_Manha"))
    .withColumn("pu_base_manha", para_decimal("PU_Base_Manha"))
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
# MAGIC # Remoção de registros já existentes

# COMMAND ----------

# DBTITLE 1,Drop Existing Tesouro Direto Tax Rate Table if Needed
# spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA_SILVER}.preco_taxa_tesouro_direto") # Caso queira limpar a tabela Bronze

# COMMAND ----------

# DBTITLE 1,Create Silver Table for Tesouro Direto Price and Rate D ...
# Criar a tabela se não existir
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA_SILVER}.preco_taxa_tesouro_direto (
        tipo_titulo STRING COMMENT 'Tipo/nome do título do Tesouro Direto (ex: Tesouro Selic, Tesouro IPCA+)',
        data_vencimento DATE COMMENT 'Data de vencimento do título',
        data_base DATE COMMENT 'Data de referência da cotação',
        taxa_compra_manha DECIMAL(10, 2) COMMENT 'Taxa de compra na cotação da manhã (percentual ao ano)',
        taxa_venda_manha DECIMAL(10, 2) COMMENT 'Taxa de venda na cotação da manhã (percentual ao ano)',
        pu_compra_manha DECIMAL(10, 2) COMMENT 'Preço Unitário de compra na cotação da manhã (em reais)',
        pu_venda_manha DECIMAL(10, 2) COMMENT 'Preço Unitário de venda na cotação da manhã (em reais)',
        pu_base_manha DECIMAL(10, 2) COMMENT 'Preço Unitário base na cotação da manhã (em reais)',
        _ingestion_timestamp TIMESTAMP COMMENT 'Timestamp de ingestão do registro no lakehouse',
        _source_file STRING COMMENT 'Nome do arquivo CSV fonte do registro',
        PRIMARY KEY (tipo_titulo, data_vencimento, data_base)
    )
    USING DELTA
    PARTITIONED BY (tipo_titulo, data_vencimento)
    COMMENT 'Tabela Silver com dados limpos e padronizados de preços e taxas do Tesouro Direto. Dados com tipos convertidos, duplicatas removidas e padronização aplicada.'
""")


# COMMAND ----------

existing_df = spark.table(f"{CATALOG}.{SCHEMA_SILVER}.preco_taxa_tesouro_direto")

# Mantém apenas linhas que ainda não existem na tabela Bronze. Isso evita reprocessamento
# e preserva a idempotência da ingestão.
df_silver = df_silver.join(
    existing_df,
    on=["Tipo_Titulo", "Data_Vencimento", "Data_Base"],
    how="left_anti"
)

print(f"Total de registros novos (após remoção de duplicados): {df_silver.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2) Remoção de duplicatas
# MAGIC
# MAGIC A combinação `(tipo_titulo, data_vencimento, data_base)` identifica unicamente uma
# MAGIC cotação diária de um título. Apesar dessa combinação ser utilizada para definir
# MAGIC a chave primária da tabela dado bruto da camada Bronze, o que evitaria duplicatas por
# MAGIC definição, mas realizarei esse processo como forma de exercício de boas práticas,
# MAGIC assim os registros duplicados nessa chave são removidos, mantendo a
# MAGIC primeira ocorrência.
# MAGIC
# MAGIC A execulção do `left_anti` nos garante a entrada de novos registros não gerarão registros duplidados, pois impedirá a gravação de já existentes.

# COMMAND ----------

linhas_antes = df_silver.count()

df_silver = df_silver.dropDuplicates(["tipo_titulo", "data_vencimento", "data_base"])

linhas_depois = df_silver.count()
print(f"Duplicatas removidas: {linhas_antes - linhas_depois}")

# COMMAND ----------

# MAGIC %md
# MAGIC
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
    #& (F.col("pu_base_manha") > 0)
)

linhas_depois = df_silver.count()
print(f"Linhas descartadas por qualidade: {linhas_antes - linhas_depois}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Por que não aplicar o filtro `(F.col("pu_base_manha") > 0)`?**
# MAGIC
# MAGIC No domínio do Tesouro Direto, o PU (Preço Unitário) não assume valor zero. Quando aparecem registros com `pu_base_manha = 0.0`, na verdade são valores extremamente baixos que foram truncados durante o processamento. Um filtro `> 0` descartaria esses registros, porém eles representam **valores válidos** e devem ser mantidos na análise.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gravação da tabela Silver (Delta)

# COMMAND ----------

(
    df_silver.write
    .format("delta")
    .mode("append")
    .saveAsTable(f"{CATALOG}.{SCHEMA_SILVER}.preco_taxa_tesouro_direto")
)

display(spark.table(f"{CATALOG}.{SCHEMA_SILVER}.preco_taxa_tesouro_direto").limit(10))