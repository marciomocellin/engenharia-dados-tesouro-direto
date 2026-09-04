# Instrucoes do projeto

## Contexto e objetivo

Este repositorio implementa um MVP de Engenharia de Dados para analisar historico de precos e taxas do Tesouro Direto em uma plataforma de nuvem. O objetivo e construir um pipeline reproduzivel, documentado e executavel que transforme dados brutos em respostas para perguntas de negocio.

A plataforma recomendada e o Databricks Free Edition, com Unity Catalog, Volumes, PySpark, SQL e Delta Lake. Outras plataformas de nuvem podem ser consideradas apenas quando solicitadas explicitamente.

Fonte oficial dos dados:
- CSV: https://www.tesourotransparente.gov.br/ckan/dataset/df56aa42-484a-4a59-8184-7676580c81e3/resource/796d2059-14e9-44e3-80c9-2d9e30b405c1/download/precotaxatesourodireto.csv
- Dicionario oficial: https://www.tesourotransparente.gov.br/ckan/dataset/df56aa42-484a-4a59-8184-7676580c81e3/resource/1a8eb2e3-4902-4a38-a1eb-6410f23d90de/download/taxa.pdf

Os dados sao abertos, agregados por titulo e nao contem dados pessoais ou sensiveis. Preserve a citacao da fonte na documentacao.

## Arquitetura e nomenclatura

Siga a Arquitetura Medalhao:

- Bronze: dado bruto preservado, sem transformacoes de negocio.
- Silver: dados limpos, tipados, deduplicados e padronizados.
- Gold: dados modelados para consumo analitico, com fatos, dimensoes e metricas.

Use o catalogo `tesouro_direto` e os schemas `bronze`, `silver` e `gold`:

- `tesouro_direto.bronze.preco_taxa_tesouro_direto`
- `tesouro_direto.silver.preco_taxa_tesouro_direto`
- `tesouro_direto.gold.dim_titulo`
- `tesouro_direto.gold.dim_data`
- `tesouro_direto.gold.fato_cotacao_diaria`

Mantenha a granularidade declarada: uma linha da fato representa uma cotacao diaria de um titulo. Consulte `docs/catalogo_dados.md` antes de alterar nomes, tipos, chaves ou linhagem.

## Organizacao dos notebooks

Mantenha um notebook Databricks por etapa, seguindo a numeracao existente:

1. `notebooks/01_bronze_ingestao.py`: le o CSV do Volume e grava o dado bruto.
2. `notebooks/02_silver_limpeza.py`: converte tipos, remove duplicatas e trata registros invalidos.
3. `notebooks/03_gold_modelagem.py`: cria as dimensoes e a tabela fato do esquema estrela.
4. `notebooks/04_qualidade_dados.py`: mede completude, consistencia, unicidade, acuracia e outliers.
5. `notebooks/05_analise_perguntas.py`: responde as perguntas de negocio usando a Gold.

Preserve o formato de notebook Databricks (`# Databricks notebook source`, `# COMMAND ----------`, `# MAGIC`). Ao criar ou editar notebooks JSON, use um documento valido com uma propriedade `cells`; cada celula deve ter `metadata.language` (`markdown` ou `python`) e celulas existentes devem manter `metadata.id`.

## Regras de ingestao e transformacao

- Bronze deve manter as colunas originais e os valores como recebidos. Metadados de controle, como `_ingestion_timestamp` e `_source_file`, sao permitidos.
- O arquivo de origem usa separador `;`, numeros com virgula decimal e datas no formato `dd/MM/yyyy`.
- Na Silver, use nomes em `snake_case`, conversao explicita de datas e numeros e documente toda regra de descarte.
- A chave de negocio da cotacao e `(tipo_titulo, data_vencimento, data_base)`.
- Registros sem `tipo_titulo`, `data_base`, `data_vencimento` ou `pu_base_manha` valido nao devem chegar a Silver.
- `pu_base_manha` deve ser positivo. Nulos de taxas ou precos de compra/venda podem ser legitimos quando nao houve operacao e nao devem ser removidos sem justificativa baseada na fonte.
- Nao remova outliers de mercado automaticamente. Meça-os, documente-os e preserve-os quando forem plausiveis.
- Use Delta e `saveAsTable` nas tabelas do Lakehouse. Evite caminhos ou nomes alternativos sem atualizar a documentacao e a linhagem.
- Evite `collect()` e processamento local para transformar dados; prefira operacoes Spark distribuiveis.

## Qualidade e analise

Toda alteracao que impacte dados deve manter ou atualizar verificacoes de:

- completude e valores nulos;
- consistencia de datas, numeros e categorias;
- unicidade pela chave de negocio;
- acuracia, incluindo positividade de precos e coerencia de prazos;
- outliers, sem confundir valor extremo com erro.

As analises devem responder, quando aplicavel, a estas perguntas:

1. Qual indexador oferece a maior taxa de compra media?
2. Como a taxa varia por faixa de prazo?
3. Como o PU evolui por indexador ao longo do tempo?
4. Existe diferenca de volatilidade do PU entre indexadores?
5. Qual titulo possui o maior historico de cotacoes?

Nao apresente resultados inventados. Quando a execucao no Databricks nao estiver disponivel, deixe claro que os numeros e screenshots ainda precisam ser obtidos na plataforma.

## Documentacao e entrega

Ao alterar o pipeline, atualize a documentacao afetada, especialmente:

- `README.md`, com objetivo, fonte, carga, modelagem, qualidade, analise e autoavaliacao;
- `docs/catalogo_dados.md`, com tabelas, campos, tipos, dominios e linhagem;
- `docs/screenshots/README.md`, quando forem necessarias evidencias visuais.

O README deve manter secoes explicitas para contexto de negocio, carga, modelagem/catalogo, pipeline, qualidade, analise e autoavaliacao. Screenshots devem evidenciar a carga no ambiente de nuvem, tabelas do catalogo, execucao do pipeline e resultados das perguntas. Videos e audios nao substituem essas evidencias.

## Fluxo de trabalho do agente

Antes de editar, leia o notebook ou documento diretamente relacionado e identifique a camada proprietaria do comportamento. Faca a menor alteracao coerente com os padroes existentes.

Depois de editar:

1. valide sintaxe Python dos notebooks alterados quando possivel;
2. execute testes, consultas ou verificacoes de qualidade relevantes;
3. confira nomes de tabelas, schemas, colunas e caminhos;
4. atualize documentacao e evidencias quando o comportamento mudar.

Nao alegue que o pipeline foi executado no Databricks sem evidencias. Nao altere arquivos ou requisitos nao relacionados ao objetivo da tarefa.
