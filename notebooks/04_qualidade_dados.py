# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Qualidade de Dados
# MAGIC
# MAGIC Verificação de qualidade executada sobre a camada Bronze (dado como chegou da fonte),
# MAGIC documentando os problemas encontrados e como foram tratados na camada Silver
# MAGIC (ver `notebooks/02_silver_limpeza.py`).

# COMMAND ----------

CATALOG = "tesouro_direto"

df_bronze = spark.table(f"{CATALOG}.bronze.preco_taxa_tesouro_direto")
df_silver = spark.table(f"{CATALOG}.silver.preco_taxa_tesouro_direto")

# COMMAND ----------

from pyspark.sql import functions as F

# MAGIC %md
# MAGIC ## Completude
# MAGIC
# MAGIC Percentual de valores nulos/vazios por coluna na Bronze.

# COMMAND ----------

total = df_bronze.count()

completude = df_bronze.select([
    (F.sum(F.when(F.col(c).isNull() | (F.trim(F.col(c)) == ""), 1).otherwise(0)) / total * 100)
    .alias(c)
    for c in df_bronze.columns
    if not c.startswith("_")
])

display(completude)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Unicidade
# MAGIC
# MAGIC Quantidade de linhas duplicadas considerando a chave de negócio
# MAGIC `(tipo_titulo, data_vencimento, data_base)`.

# COMMAND ----------

duplicatas = (
    df_bronze.groupBy("Tipo Titulo", "Data Vencimento", "Data Base")
    .count()
    .filter("count > 1")
)
print("Grupos de chave duplicada encontrados na Bronze:", duplicatas.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Consistência
# MAGIC
# MAGIC Validação de que as datas seguem o formato `dd/MM/yyyy` e que os números seguem o
# MAGIC padrão brasileiro (vírgula decimal). Linhas que não seguem o padrão viram `null` após a
# MAGIC conversão feita na Silver e são contabilizadas abaixo.

# COMMAND ----------

datas_invalidas = df_bronze.filter(
    F.to_date(F.col("Data Base"), "dd/MM/yyyy").isNull() & F.col("Data Base").isNotNull()
).count()
print("Datas Base com formato inválido:", datas_invalidas)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Acurácia / Outliers
# MAGIC
# MAGIC Taxas e preços unitários devem ser positivos. Valores negativos ou nulos após a
# MAGIC conversão numérica indicam erro de captura na fonte e são descartados na Silver.
# MAGIC Também avaliamos outliers extremos de PU (fora de 3 desvios padrão da média) apenas
# MAGIC para conhecimento, sem removê-los, pois oscilações de mercado são esperadas.

# COMMAND ----------

stats = df_silver.select(
    F.mean("pu_base_manha").alias("media"), F.stddev("pu_base_manha").alias("desvio")
).first()

limite_superior = stats["media"] + 3 * stats["desvio"]
limite_inferior = stats["media"] - 3 * stats["desvio"]

outliers = df_silver.filter(
    (F.col("pu_base_manha") > limite_superior) | (F.col("pu_base_manha") < limite_inferior)
).count()

print(f"Outliers de PU Base Manha (fora de 3 desvios padrão): {outliers}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumo das ações de qualidade tomadas na camada Silver
# MAGIC
# MAGIC | Problema | Ação |
# MAGIC | --- | --- |
# MAGIC | Registros com chave de negócio duplicada | `dropDuplicates` na chave `(tipo_titulo, data_vencimento, data_base)` |
# MAGIC | `tipo_titulo`, `data_base` ou `data_vencimento` nulos | Linha descartada (sem valor analítico) |
# MAGIC | Datas em formato inesperado | Convertidas para `null` via `to_date` com máscara `dd/MM/yyyy`, depois descartadas |
# MAGIC | Números com vírgula decimal (padrão BR) | Convertidos para `double` com `.` como separador |
# MAGIC | `pu_base_manha` nulo ou <= 0 | Linha descartada, pois preço unitário não pode ser nulo/negativo |
# MAGIC | Espaços extras em `tipo_titulo` | `trim()` aplicado para padronizar |
