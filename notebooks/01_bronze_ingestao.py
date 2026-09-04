# Databricks notebook source
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
# MAGIC ## Parâmetros

# COMMAND ----------

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

CATALOG = "tesouro_direto"
SCHEMA_BRONZE = "bronze"
VOLUME_PATH = "/Volumes/tesouro_direto/bronze/arquivos_brutos"
ARQUIVO_ORIGEM = "PrecoTaxaTesouroDireto.csv"

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA_BRONZE}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA_BRONZE}.arquivos_brutos")

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

# MAGIC %md
# MAGIC ## Gravação da tabela Bronze (Delta)

# COMMAND ----------

(
    df_bronze_final.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(f"{CATALOG}.{SCHEMA_BRONZE}.preco_taxa_tesouro_direto")
)

display(spark.table(f"{CATALOG}.{SCHEMA_BRONZE}.preco_taxa_tesouro_direto").limit(10))

# COMMAND ----------

print(
    "Total de registros carregados na Bronze:",
    spark.table(f"{CATALOG}.{SCHEMA_BRONZE}.preco_taxa_tesouro_direto").count(),
)
