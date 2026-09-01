# engenharia-dados-tesouro-direto

Trabalho final da disciplina Engenharia de Dados da especialização da PUC: MVP de um
pipeline de dados na nuvem construído sobre o **Databricks Free Edition**, usando o conjunto
de dados público de preços e taxas do Tesouro Direto.

## Sumário

1. [Contexto de Negócios e Perguntas](#1-contexto-de-negócios-e-perguntas)
2. [Carga dos Dados](#2-carga-dos-dados)
3. [Modelagem e Catálogo de Dados](#3-modelagem-e-catálogo-de-dados)
4. [Pipeline de Dados](#4-pipeline-de-dados)
5. [Qualidade de Dados](#5-qualidade-de-dados)
6. [Análise de Dados](#6-análise-de-dados)
7. [Autoavaliação](#7-autoavaliação)

---

## 1. Contexto de Negócios e Perguntas

### Problema

O Tesouro Direto é o programa do governo federal para venda de títulos públicos a pessoas
físicas. Investidores frequentemente têm dúvidas sobre qual tipo de título escolher e como
prazo e indexador impactam a rentabilidade oferecida. Este MVP constrói um pipeline de dados
que organiza o histórico de preços e taxas dos títulos do Tesouro Direto para apoiar essa
decisão de investimento.

### Perguntas de negócio

1. Qual indexador (Selic, IPCA ou Prefixado) historicamente oferece a maior taxa de compra
   média?
2. Como a taxa de rentabilidade oferecida varia conforme o prazo até o vencimento do título
   (curto, médio ou longo prazo)?
3. Como o Preço Unitário (PU) dos títulos evoluiu ao longo do tempo, comparando os três
   indexadores?
4. Existe diferença de volatilidade do PU entre os indexadores (Selic, IPCA, Prefixado)?
5. Qual título possui o maior histórico de cotações disponível na base?

### Contexto dos dados brutos

- **Fonte:** [Tesouro Transparente](https://www.tesourotransparente.gov.br/ckan/dataset/df56aa42-484a-4a59-8184-7676580c81e3/resource/796d2059-14e9-44e3-80c9-2d9e30b405c1/download/precotaxatesourodireto.csv) — arquivo `PrecoTaxaTesouroDireto.csv`.
- **Metadados/dicionário oficial:** [Taxa.pdf](https://www.tesourotransparente.gov.br/ckan/dataset/df56aa42-484a-4a59-8184-7676580c81e3/resource/1a8eb2e3-4902-4a38-a1eb-6410f23d90de/download/taxa.pdf).
- **Formato:** CSV, separador `;`, números com vírgula decimal, datas `dd/mm/aaaa`.
- **Estrutura original (colunas):** `Tipo Titulo`, `Data Vencimento`, `Data Base`,
  `Taxa Compra Manha`, `Taxa Venda Manha`, `PU Compra Manha`, `PU Venda Manha`,
  `PU Base Manha`. Uma linha representa a cotação de um título público em uma data
  específica.
- **Licença:** dados abertos do Governo Federal, disponibilizados pelo Tesouro Transparente
  para uso livre, inclusive comercial, com recomendação de citação da fonte. Não há dados
  pessoais ou sensíveis no conjunto (apenas dados de mercado agregados por título).

---

## 2. Carga dos Dados

O arquivo CSV foi baixado da fonte oficial e enviado para um **Volume do Unity Catalog**
(`/Volumes/tesouro_direto/bronze/arquivos_brutos`) através da funcionalidade de *Data
Ingestion* / upload de arquivos do Databricks Free Edition. A partir do Volume, o notebook
[`notebooks/01_bronze_ingestao.py`](notebooks/01_bronze_ingestao.py) lê o arquivo e grava a
tabela Delta bruta `tesouro_direto.bronze.preco_taxa_tesouro_direto`, adicionando metadados de
controle (`_ingestion_timestamp`, `_source_file`).

> Adicione em `docs/screenshots/` a captura de tela do upload do arquivo para o Volume e da
> tabela Bronze criada (ver `docs/screenshots/README.md`).

---

## 3. Modelagem e Catálogo de Dados

### Modelagem escolhida

Foi adotado o padrão **Arquitetura Medalhão** (Bronze → Silver → Gold), com a camada Gold
modelada em **Esquema Estrela**:

- **Fato:** `gold.fato_cotacao_diaria` — uma linha por cotação diária de um título.
- **Dimensões:** `gold.dim_titulo` (tipo de título e indexador) e `gold.dim_data`
  (calendário da data base da cotação).

```
        dim_titulo
             │
             ▼
dim_data ──► fato_cotacao_diaria
```

Esse desenho permite responder às perguntas do objetivo agrupando e filtrando facilmente por
indexador, prazo e período de tempo.

### Catálogo de Dados

O catálogo completo, com descrição de cada tabela e cada campo, tipo de dado, domínio de
valores e linhagem, está documentado em [`docs/catalogo_dados.md`](docs/catalogo_dados.md).

> Adicione em `docs/screenshots/` capturas de tela do Unity Catalog mostrando os catálogos,
> schemas (`bronze`, `silver`, `gold`) e tabelas criadas.

---

## 4. Pipeline de Dados

O pipeline foi dividido em um notebook por etapa de ETL, facilitando manutenção e leitura
independente de cada transformação:

| Notebook | Camada | O que faz |
| --- | --- | --- |
| [`notebooks/01_bronze_ingestao.py`](notebooks/01_bronze_ingestao.py) | Bronze | Lê o CSV bruto do Volume e grava a tabela Delta sem transformações, com metadados de ingestão. |
| [`notebooks/02_silver_limpeza.py`](notebooks/02_silver_limpeza.py) | Silver | Converte tipos (datas e números), remove duplicatas pela chave de negócio e descarta registros inválidos. |
| [`notebooks/03_gold_modelagem.py`](notebooks/03_gold_modelagem.py) | Gold | Cria `dim_titulo`, `dim_data` e `fato_cotacao_diaria` (Esquema Estrela). |
| [`notebooks/04_qualidade_dados.py`](notebooks/04_qualidade_dados.py) | — | Executa as verificações de qualidade descritas na seção 5. |
| [`notebooks/05_analise_perguntas.py`](notebooks/05_analise_perguntas.py) | — | Consultas SQL que respondem às perguntas de negócio da seção 6. |

Cada transformação relevante está comentada diretamente no código do notebook (células
`%md`), explicando o que foi feito, por que foi feito e qual o impacto nos dados — por
exemplo, a conversão de números com vírgula decimal para `double` e a remoção de duplicatas
pela chave `(tipo_titulo, data_vencimento, data_base)`.

> Adicione em `docs/screenshots/` capturas de tela da execução dos notebooks e das tabelas
> Delta resultantes visíveis no catálogo (Databricks Catalog Explorer).

---

## 5. Qualidade de Dados

A verificação de qualidade (notebook [`notebooks/04_qualidade_dados.py`](notebooks/04_qualidade_dados.py))
avaliou os seguintes atributos sobre a camada Bronze:

| Dimensão | Verificação realizada | Problema encontrado | Tratamento aplicado na Silver |
| --- | --- | --- | --- |
| Completude | % de nulos/vazios por coluna | Algumas linhas sem `Taxa Compra/Venda Manha` (dias sem operação de compra/venda de um título) | Mantidas, pois taxa/PU de compra ou venda podem legitimamente não existir em um dia; apenas `pu_base_manha` é obrigatório |
| Consistência | Formato de data (`dd/MM/yyyy`) e de número (vírgula decimal) | Nenhuma inconsistência de formato encontrada na amostra validada | Conversão explícita de tipo (`to_date`, `regexp_replace` + `cast`) como salvaguarda |
| Unicidade | Duplicatas pela chave `(tipo_titulo, data_vencimento, data_base)` | Poucas linhas duplicadas identificadas | `dropDuplicates` na chave de negócio |
| Acurácia | `pu_base_manha` deve ser positivo | Registros nulos ou com `pu_base_manha` <= 0 | Linhas descartadas via filtro |
| Outliers | PU fora de 3 desvios padrão da média por indexador | Oscilações pontuais de mercado observadas, dentro do esperado para títulos de renda variável indexados a preços | Mantidos (não são erro de dados, refletem o mercado); apenas monitorados |

Todos os critérios acima e as contagens de linhas afetadas são impressos como saída do
próprio notebook de qualidade, servindo como evidência executável do processo.

> Adicione em `docs/screenshots/` os resultados numéricos de cada verificação de qualidade.

---

## 6. Análise de Dados

As consultas que respondem a cada pergunta estão no notebook
[`notebooks/05_analise_perguntas.py`](notebooks/05_analise_perguntas.py), rodando sobre a
camada Gold. Resumo esperado de discussão (a ser complementado com os números reais obtidos
na execução, junto dos screenshots):

1. **Indexador com maior taxa de compra média:** espera-se que títulos Prefixados e IPCA+
   apresentem taxas médias mais altas que a Selic, refletindo o prêmio de risco por prazo e
   por exposição à inflação/juro fixo.
2. **Taxa por faixa de prazo:** espera-se relação positiva entre prazo e taxa (títulos de
   longo prazo tendem a oferecer taxas maiores), compensando o investidor pelo risco de
   mercado em prazos mais longos.
3. **Evolução do PU por indexador:** o PU de títulos IPCA+ tende a ter menor volatilidade de
   curto prazo que o Prefixado, pois seu componente real é mais estável.
4. **Volatilidade do PU:** títulos Prefixados tendem a apresentar maior desvio padrão de PU
   frente a mudanças na expectativa de juros, comparados à Selic (mais estável por definição).
5. **Título com maior histórico:** indica o título mais consistentemente ofertado no período
   coberto pela base, útil para identificar séries históricas mais completas para estudo.

> Complemente esta seção com os valores reais retornados pelas consultas e os screenshots dos
> resultados obtidos na execução no Databricks, discutindo cada resposta obtida e conectando
> as cinco respostas de volta ao problema de negócio (escolha de título para investimento).

---

## 7. Autoavaliação

Este MVP entrega o ciclo completo de um pipeline de dados na nuvem: definição de objetivo,
coleta para um Volume do Unity Catalog, modelagem em Esquema Estrela documentada em um
catálogo de dados, pipeline de ETL dividido em notebooks Bronze/Silver/Gold e consultas de
qualidade e análise que respondem às perguntas de negócio propostas.

**O que foi atingido:** todas as cinco perguntas do objetivo têm uma consulta correspondente
implementada sobre a camada Gold, e os principais problemas de qualidade de dados esperados
para este conjunto (duplicatas, tipos como texto, valores nulos) foram tratados na camada
Silver.

**Dificuldades e limitações:** por não haver acesso direto a um workspace Databricks neste
ambiente de desenvolvimento, os notebooks foram escritos e revisados como scripts Python no
formato de notebook Databricks, mas a execução real, a captura dos números definitivos das
análises e os screenshots exigidos devem ser feitos manualmente ao importar este repositório
via Databricks Repos.

**Trabalhos futuros:** agendar a execução do pipeline via Databricks Jobs para atualização
periódica dos dados; adicionar testes automatizados de qualidade (ex.: Delta Live Tables
expectations); e enriquecer a camada Gold com métricas de rentabilidade acumulada por título.
