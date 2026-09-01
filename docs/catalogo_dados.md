# Catálogo de Dados

Documentação das tabelas do Lakehouse do projeto, organizadas segundo a Arquitetura Medalhão
(`bronze` → `silver` → `gold`). Todas as tabelas residem no catálogo Unity Catalog
`tesouro_direto`.

## Linhagem geral

```
precotaxatesourodireto.csv (Tesouro Transparente)
        │  (notebooks/01_bronze_ingestao.py)
        ▼
tesouro_direto.bronze.preco_taxa_tesouro_direto
        │  (notebooks/02_silver_limpeza.py)
        ▼
tesouro_direto.silver.preco_taxa_tesouro_direto
        │  (notebooks/03_gold_modelagem.py)
        ▼
tesouro_direto.gold.dim_titulo
tesouro_direto.gold.dim_data
tesouro_direto.gold.fato_cotacao_diaria
```

---

## `bronze.preco_taxa_tesouro_direto`

**Descrição:** dado bruto, exatamente como recebido do arquivo `PrecoTaxaTesouroDireto.csv`,
apenas com colunas de controle de ingestão adicionadas. Todos os campos originais são
armazenados como texto (`string`).

| Campo | Tipo | Descrição | Domínio / Observações |
| --- | --- | --- | --- |
| `Tipo Titulo` | string | Nome do título público (ex.: "Tesouro Selic 2029") | Texto livre, conforme fonte |
| `Data Vencimento` | string | Data de vencimento do título, formato `dd/MM/yyyy` | Texto, convertido na Silver |
| `Data Base` | string | Data de referência da cotação, formato `dd/MM/yyyy` | Texto, convertido na Silver |
| `Taxa Compra Manha` | string | Taxa de compra (% a.a.), vírgula decimal | Texto, convertido na Silver |
| `Taxa Venda Manha` | string | Taxa de venda (% a.a.), vírgula decimal | Texto, convertido na Silver |
| `PU Compra Manha` | string | Preço Unitário de compra (R$), vírgula decimal | Texto, convertido na Silver |
| `PU Venda Manha` | string | Preço Unitário de venda (R$), vírgula decimal | Texto, convertido na Silver |
| `PU Base Manha` | string | Preço Unitário base (R$), vírgula decimal | Texto, convertido na Silver |
| `_ingestion_timestamp` | timestamp | Data/hora em que o registro foi carregado no Lakehouse | Metadado de controle |
| `_source_file` | string | Nome do arquivo de origem | Metadado de controle |

**Linhagem:** carga direta do arquivo CSV disponibilizado pelo Tesouro Transparente.

---

## `silver.preco_taxa_tesouro_direto`

**Descrição:** dado limpo, tipado e padronizado, sem duplicatas e sem registros inválidos
(ver seção *Qualidade de Dados* no `README.md` para detalhes dos critérios de descarte).

| Campo | Tipo | Descrição | Domínio / Observações |
| --- | --- | --- | --- |
| `tipo_titulo` | string | Nome do título público, sem espaços extras | Ex.: "Tesouro Selic", "Tesouro IPCA+", "Tesouro Prefixado" |
| `data_vencimento` | date | Data de vencimento do título | >= `data_base` |
| `data_base` | date | Data de referência da cotação | Datas do período disponibilizado pela fonte |
| `taxa_compra_manha` | double | Taxa de compra (% a.a.) | Pode ser nula para títulos sem operação de compra no dia |
| `taxa_venda_manha` | double | Taxa de venda (% a.a.) | Pode ser nula para títulos sem operação de venda no dia |
| `pu_compra_manha` | double | Preço Unitário de compra (R$) | > 0 quando não nulo |
| `pu_venda_manha` | double | Preço Unitário de venda (R$) | > 0 quando não nulo |
| `pu_base_manha` | double | Preço Unitário base (R$) | Sempre > 0 (obrigatório, filtrado na limpeza) |
| `_ingestion_timestamp` | timestamp | Herdado da Bronze | Metadado de controle |
| `_source_file` | string | Herdado da Bronze | Metadado de controle |

**Linhagem:** `bronze.preco_taxa_tesouro_direto` → conversão de tipos, remoção de duplicatas
pela chave `(tipo_titulo, data_vencimento, data_base)` e descarte de registros com campos
obrigatórios nulos ou preço unitário inválido.

---

## `gold.dim_titulo`

**Descrição:** dimensão com os tipos de título distintos e seu indexador de rentabilidade.

| Campo | Tipo | Descrição | Domínio / Observações |
| --- | --- | --- | --- |
| `sk_titulo` | int | Chave substituta (surrogate key) da dimensão | Sequencial, gerado via `row_number()` |
| `tipo_titulo` | string | Nome do título público | Chave de negócio |
| `indexador` | string | Classificação derivada do nome do título | Um de: `Selic`, `IPCA`, `Prefixado` |

**Linhagem:** `silver.preco_taxa_tesouro_direto.tipo_titulo` (valores distintos) + regra de
classificação por palavra-chave no nome do título.

---

## `gold.dim_data`

**Descrição:** dimensão de calendário com as datas base presentes no conjunto de dados.

| Campo | Tipo | Descrição | Domínio / Observações |
| --- | --- | --- | --- |
| `sk_data` | int | Chave substituta no formato `yyyyMMdd` | Ex.: `20240115` |
| `data` | date | Data calendário | Datas do período disponibilizado pela fonte |
| `ano` | int | Ano da data | Ex.: 2024 |
| `mes` | int | Mês da data | 1 a 12 |
| `trimestre` | int | Trimestre da data | 1 a 4 |
| `dia_semana` | string | Nome do dia da semana | Ex.: "Monday" |

**Linhagem:** `silver.preco_taxa_tesouro_direto.data_base` (valores distintos).

---

## `gold.fato_cotacao_diaria`

**Descrição:** tabela fato com granularidade de uma linha por cotação diária de um título,
pronta para responder às perguntas de negócio do MVP.

| Campo | Tipo | Descrição | Domínio / Observações |
| --- | --- | --- | --- |
| `sk_titulo` | int | Chave estrangeira para `dim_titulo` | — |
| `sk_data` | int | Chave estrangeira para `dim_data` (data base da cotação) | — |
| `data_vencimento` | date | Data de vencimento do título nesta cotação | — |
| `prazo_dias` | int | Diferença em dias entre `data_vencimento` e a data base | >= 0 esperado |
| `taxa_compra_manha` | double | Taxa de compra (% a.a.) | Métrica |
| `taxa_venda_manha` | double | Taxa de venda (% a.a.) | Métrica |
| `pu_compra_manha` | double | Preço Unitário de compra (R$) | Métrica |
| `pu_venda_manha` | double | Preço Unitário de venda (R$) | Métrica |
| `pu_base_manha` | double | Preço Unitário base (R$) | Métrica |

**Linhagem:** `silver.preco_taxa_tesouro_direto` `JOIN` `gold.dim_titulo` (por `tipo_titulo`),
com `sk_data` calculado a partir de `data_base` e `prazo_dias` calculado via `datediff`.
