# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 01 - Bronze: Ingestão dos dados brutos do Tesouro Direto
# MAGIC
# MAGIC **Objetivo desta etapa:** trazer o arquivo `PrecoTaxaTesouroDireto.csv`, publicado pelo
# MAGIC Tesouro Transparente, para dentro do ambiente Databricks (Unity Catalog Volumes) sem
# MAGIC nenhuma transformação, preservando o dado exatamente como foi recebido da fonte, além de
# MAGIC metadados de controle de ingestão (data/hora de carga e arquivo de origem).
# MAGIC
# MAGIC Fonte: https://www.tesourotransparente.gov.br/ckan/dataset/df56aa42-484a-4a59-8184-7676580c81e3/resource/796d2059-14e9-44e3-80c9-2d9e30b405c1/download/precotaxatesourodireto.csv
# MAGIC
# MAGIC Licença: Dados Abertos do Governo Federal (Tesouro Transparente), uso livre com citação da fonte.

# COMMAND ----------

# MAGIC %md
# MAGIC # Salvaguarda para execulção em ambiente local.

# COMMAND ----------

# DBTITLE 1,Initialize Spark Session for Local Development
try:
    spark
except NameError:
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("ExemploSparkLocal")
        .getOrCreate()
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parâmetros

# COMMAND ----------


CATALOG = "tesouro_direto"
SCHEMA_BRONZE = "bronze"
VOLUME_PATH = "/Volumes/tesouro_direto/bronze/arquivos_brutos"
ARQUIVO_ORIGEM = "precotaxatesourodireto.csv"

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA_BRONZE}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA_BRONZE}.arquivos_brutos")

# COMMAND ----------

# MAGIC %md
# MAGIC Foi criado um Volume (/Volumes/tesouro_direto/bronze/arquivos_brutos) para guardar o CSV, então adicionei o arquivo baixado do Tesouro Transparente.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Leitura do CSV bruto
# MAGIC
# MAGIC O arquivo é disponibilizado com separador `;`, números com vírgula decimal e datas no
# MAGIC formato `dd/mm/aaaa`. Nesta camada Bronze, **nada é convertido**: todas as colunas são
# MAGIC lidas como texto (`StringType`) para preservar fielmente o dado original.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

schema_bruto = StructType([
    StructField("Tipo Titulo", StringType(), True),
    StructField("Data Vencimento", StringType(), True),
    StructField("Data Base", StringType(), True),
    StructField("Taxa Compra Manha", StringType(), True),
    StructField("Taxa Venda Manha", StringType(), True),
    StructField("PU Compra Manha", StringType(), True),
    StructField("PU Venda Manha", StringType(), True),
    StructField("PU Base Manha", StringType(), True),
])

df_bronze = (
    spark.read
    .option("header", "true")
    .option("delimiter", ";")
    .schema(schema_bruto)
    .csv(f"{VOLUME_PATH}/{ARQUIVO_ORIGEM}")
)

# COMMAND ----------

df_bronze.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Adição de metadados de controle
# MAGIC
# MAGIC - `_ingestion_timestamp`: quando o registro foi carregado no Lakehouse.
# MAGIC - `_source_file`: nome do arquivo de origem, permitindo rastreabilidade.

# COMMAND ----------

df_bronze_final = (
    df_bronze
    .withColumn("_ingestion_timestamp", F.current_timestamp())
    .withColumn("_source_file", F.lit(ARQUIVO_ORIGEM))
)

# COMMAND ----------

df_bronze_final.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gravação da tabela Bronze (Delta)

# COMMAND ----------

# DBTITLE 1,Remove tabela preco taxa tesouro direto se existir
# spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA_BRONZE}.preco_taxa_tesouro_direto")

# COMMAND ----------

# DBTITLE 1,Cria a tabela se não existir
# Cria a tabela se não existir
spark.sql(f'''\n
CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA_BRONZE}.preco_taxa_tesouro_direto (
    Tipo_Titulo STRING,
    Data_Vencimento STRING,
    Data_Base STRING,
    Taxa_Compra_Manha STRING,
    Taxa_Venda_Manha STRING,
    PU_Compra_Manha STRING,
    PU_Venda_Manha STRING,
    PU_Base_Manha STRING,
    _ingestion_timestamp TIMESTAMP,
    _source_file STRING,
    PRIMARY KEY (Tipo_Titulo, Data_Vencimento, Data_Base)
)
USING DELTA
COMMENT 'A tabela contém dados sobre as taxas e preços dos títulos do Tesouro Direto. Os principais elementos incluem informações sobre o tipo de título, datas de vencimento e as taxas de compra e venda pela manhã.'
PARTITIONED BY (Tipo_Titulo, Data_Vencimento)
''')

# COMMAND ----------

# Rename columns to match the table schema (underscores instead of spaces)
df_bronze_renamed = df_bronze_final.toDF(
    "Tipo_Titulo",
    "Data_Vencimento",
    "Data_Base",
    "Taxa_Compra_Manha",
    "Taxa_Venda_Manha",
    "PU_Compra_Manha",
    "PU_Venda_Manha",
    "PU_Base_Manha",
    "_ingestion_timestamp",
    "_source_file"
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Removendo as linhas que já existem na tabela
# MAGIC
# MAGIC Como o tesouro faz atualizanção incremental do CSV, então só precisaremos adicionar os novos dados a tabela .

# COMMAND ----------

# DBTITLE 1,Remove Duplicates Using Left Anti Join on Existing Data
existing_df = spark.table(f"{CATALOG}.{SCHEMA_BRONZE}.preco_taxa_tesouro_direto")

# Realiza um LEFT ANTI JOIN para manter apenas as linhas que não existem na tabela
df_bronze_renamed = df_bronze_renamed.join(
    existing_df,
    on=["Tipo_Titulo", "Data_Vencimento", "Data_Base"],
    how="left_anti"
)

print(f"Total de registros novos (após remoção de duplicados): {df_bronze_renamed.count()}")

# COMMAND ----------

# DBTITLE 1,Display Renamed DataFrame for Analysis Insights
df_bronze_renamed.display()

# COMMAND ----------

df_bronze_renamed.display()

# COMMAND ----------

# DBTITLE 1,Append Data to Bronze Preco Taxa Tesouro Direto Table

(
    df_bronze_renamed.write
    .format("delta")
    .mode("append")
    .option("delta.columnMapping.mode", "name")
    .saveAsTable(f"{CATALOG}.{SCHEMA_BRONZE}.preco_taxa_tesouro_direto")
)

display(spark.table(f"{CATALOG}.{SCHEMA_BRONZE}.preco_taxa_tesouro_direto").limit(10))

# COMMAND ----------

# DBTITLE 1,Count of Records Loaded in Bronze Preco Taxa Tesouro Di ...
print(
    "Total de registros carregados na Bronze:",
    spark.table(f"{CATALOG}.{SCHEMA_BRONZE}.preco_taxa_tesouro_direto").count(),
)