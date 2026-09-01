# Databricks notebook source
# MAGIC %md
# MAGIC # 05 - Análise: Respondendo às perguntas do MVP
# MAGIC
# MAGIC Este notebook consulta a camada Gold (Esquema Estrela) para responder às perguntas de
# MAGIC negócio definidas na seção **Objetivo** do `README.md`.

# COMMAND ----------

CATALOG = "tesouro_direto"
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql("USE SCHEMA gold")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pergunta 1 — Qual indexador (Selic, IPCA ou Prefixado) historicamente oferece a
# MAGIC ## maior taxa de compra média?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   t.indexador,
# MAGIC   ROUND(AVG(f.taxa_compra_manha), 2) AS taxa_compra_media,
# MAGIC   COUNT(*) AS qtd_cotacoes
# MAGIC FROM fato_cotacao_diaria f
# MAGIC JOIN dim_titulo t ON f.sk_titulo = t.sk_titulo
# MAGIC WHERE f.taxa_compra_manha IS NOT NULL
# MAGIC GROUP BY t.indexador
# MAGIC ORDER BY taxa_compra_media DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pergunta 2 — Como a taxa de rentabilidade varia conforme o prazo até o vencimento?
# MAGIC
# MAGIC Os títulos são agrupados em faixas de prazo (curto: até 2 anos; médio: 2 a 5 anos;
# MAGIC longo: acima de 5 anos) para observar a relação entre prazo e taxa.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   CASE
# MAGIC     WHEN prazo_dias <= 730 THEN 'Curto prazo (<= 2 anos)'
# MAGIC     WHEN prazo_dias <= 1825 THEN 'Médio prazo (2 a 5 anos)'
# MAGIC     ELSE 'Longo prazo (> 5 anos)'
# MAGIC   END AS faixa_prazo,
# MAGIC   ROUND(AVG(taxa_compra_manha), 2) AS taxa_compra_media
# MAGIC FROM fato_cotacao_diaria
# MAGIC WHERE taxa_compra_manha IS NOT NULL AND prazo_dias >= 0
# MAGIC GROUP BY faixa_prazo
# MAGIC ORDER BY taxa_compra_media DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pergunta 3 — Como o preço unitário (PU) evoluiu ao longo do tempo por indexador?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   d.ano,
# MAGIC   t.indexador,
# MAGIC   ROUND(AVG(f.pu_base_manha), 2) AS pu_medio
# MAGIC FROM fato_cotacao_diaria f
# MAGIC JOIN dim_titulo t ON f.sk_titulo = t.sk_titulo
# MAGIC JOIN dim_data d ON f.sk_data = d.sk_data
# MAGIC GROUP BY d.ano, t.indexador
# MAGIC ORDER BY d.ano, t.indexador

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pergunta 4 — Existe diferença de volatilidade do PU entre os indexadores?
# MAGIC
# MAGIC O desvio padrão do PU é usado como medida simples de volatilidade.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   t.indexador,
# MAGIC   ROUND(STDDEV(f.pu_base_manha), 2) AS desvio_padrao_pu,
# MAGIC   ROUND(AVG(f.pu_base_manha), 2) AS pu_medio
# MAGIC FROM fato_cotacao_diaria f
# MAGIC JOIN dim_titulo t ON f.sk_titulo = t.sk_titulo
# MAGIC GROUP BY t.indexador
# MAGIC ORDER BY desvio_padrao_pu DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pergunta 5 — Qual título possui o maior histórico de cotações disponíveis?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   t.tipo_titulo,
# MAGIC   COUNT(*) AS qtd_cotacoes,
# MAGIC   MIN(d.data) AS primeira_cotacao,
# MAGIC   MAX(d.data) AS ultima_cotacao
# MAGIC FROM fato_cotacao_diaria f
# MAGIC JOIN dim_titulo t ON f.sk_titulo = t.sk_titulo
# MAGIC JOIN dim_data d ON f.sk_data = d.sk_data
# MAGIC GROUP BY t.tipo_titulo
# MAGIC ORDER BY qtd_cotacoes DESC

# COMMAND ----------

# MAGIC %md
# MAGIC As discussões sobre os resultados obtidos e os screenshots das execuções destas
# MAGIC consultas estão documentados na seção **Análise de Dados** do `README.md`.
