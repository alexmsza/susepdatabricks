# Exercicios: Análise de dados da SUSEP.
## Instruções:
- Acesse o site da SUSEP (https://www2.susep.gov.br/menuestatistica/Autoseg/principal.aspx); do lado direito você vai encontrar vários dados sobre seguros de automóveis. Faça o download dos dados referente ao 2º semestre de 2020. O arquivo ZIP contém vários arquivos menores no formato CSV.
- Entregue um relatório com suas análises, assim como os scripts devidamente comentados. Envie uma cópia do trabalho para o Professor Bruno e o Professor Nelio.

- Abaixo, eu disponibilizo a função para ler os arquivos CSV:

```python
%python

def criar_view_temporaria(nome_tabela):
    # Construindo o caminho do arquivo CSV com base no nome da tabela
    caminho_csv = f"dbfs:/FileStore/{nome_tabela}.csv"
    
    # Leitura do arquivo CSV usando PySpark
    df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("sep", ';') \
        .load(caminho_csv)
    
    # Criando uma view temporária com o nome especificado
    df.createOrReplaceTempView(nome_tabela)
    
    # Exibindo as primeiras 10 linhas da view temporária
    print(f"\nTabela: {nome_tabela}")
    spark.sql(f"SELECT * FROM {nome_tabela} LIMIT 10").display()
```

Para executar a função, utilize os comandos abaixo:

```python
%python
criar_view_temporaria(nome_tabela="auto_cau")
criar_view_temporaria(nome_tabela="auto_cat")
criar_view_temporaria(nome_tabela="auto_cep")
criar_view_temporaria(nome_tabela="auto_cidade")
criar_view_temporaria(nome_tabela="auto_cob")
criar_view_temporaria(nome_tabela="auto_idade")
criar_view_temporaria(nome_tabela="auto_reg")
criar_view_temporaria(nome_tabela="auto_sexo")
criar_view_temporaria(nome_tabela="auto2_grupo")
criar_view_temporaria(nome_tabela="auto2_vei")
criar_view_temporaria(nome_tabela="PremReg")
criar_view_temporaria(nome_tabela="arq_casco_comp")
criar_view_temporaria(nome_tabela="arq_casco3_comp")
criar_view_temporaria(nome_tabela="arq_casco4_comp")
criar_view_temporaria(nome_tabela="SinReg")
```

---
# Respostas das Análises (Versão Final)

---

## Questão 2: Exploração de Dados

**Insights Iniciais:**

1.  **Distribuição de Segurados por Sexo:** Permite verificar a proporção de apólices entre homens e mulheres, identificando o público principal.
2.  **Sinistralidade por Tipo de Cobertura:** Ajuda a identificar quais coberturas (casco, danos a terceiros) concentram mais sinistros, indicando áreas de maior risco.
3.  **Distribuição Geográfica dos Sinistros:** Mostra as regiões com maior número de sinistros, auxiliando na análise de risco geográfico.

**Consulta SQL:**
```sql
-- Insight 1: Contagem de segurados por sexo
SELECT s.descricao, COUNT(a.id_apolice) AS total_apolices FROM arq_casco_comp a JOIN auto_sexo s ON a.sexo = s.codigo GROUP BY s.descricao;

-- Insight 2: Total de sinistros por tipo de cobertura
SELECT tipo_sin, SUM(numSinistros) AS total_sinistros FROM SinReg WHERE tipo_sin != 'TOTAL' GROUP BY tipo_sin ORDER BY total_sinistros DESC;

-- Insight 3: Total de sinistros por região
SELECT regiao, descricao, SUM(numSinistros) AS total_sinistros FROM SinReg WHERE tipo_sin != 'TOTAL' GROUP BY regiao, descricao ORDER BY total_sinistros DESC;
```

---

## Questão 3: Análise por Sexo

**Interpretação:**
Em vez de calcular uma média de idade, uma análise mais rica é a distribuição de segurados por **faixa etária** e sexo. Isso permite identificar qual combinação de gênero e faixa de idade possui mais apólices, por exemplo, se "Homens entre 26 e 35 anos" é o segmento mais comum.

**Consulta SQL:**
```sql
-- Pergunta 3: Distribuição de segurados por faixa etária e sexo (versão final)
SELECT
    i.descricao AS faixa_etaria,
    s.descricao AS sexo,
    COUNT(a.id_apolice) AS numero_de_apolices
FROM
    arq_casco_comp a
JOIN
    auto_idade i ON a.faixa_etaria = i.codigo
JOIN
    auto_sexo s ON a.sexo = s.codigo
WHERE
    s.codigo IN ('M', 'F') AND i.codigo != 0
GROUP BY
    i.descricao,
    s.descricao
ORDER BY
    i.descricao,
    s.descricao;
```

---

## Questão 4: Análise por Região

**Interpretação:**
Analisar a distribuição de segurados por **faixa etária dentro de cada região** permite um mapeamento demográfico detalhado. Pode-se descobrir que uma região tem uma concentração de segurados jovens, enquanto outra tem um perfil mais sênior, impactando diretamente as estratégias de produto e precificação local.

**Consulta SQL:**
```sql
-- Pergunta 4: Distribuição de segurados por faixa etária e região (versão final)
SELECT
    r.DESCRICAO AS regiao,
    i.descricao AS faixa_etaria,
    COUNT(a.id_apolice) AS numero_de_apolices
FROM
    arq_casco_comp a
JOIN
    auto_reg r ON a.regiao = r.CODIGO
JOIN
    auto_idade i ON a.faixa_etaria = i.codigo
WHERE
    i.codigo != 0
GROUP BY
    r.DESCRICAO,
    i.descricao
ORDER BY
    r.DESCRICAO,
    i.descricao;
```

---

## Questão 5: Análise por Ano do Modelo

**Interpretação:**
Ao cruzar a **faixa etária** do segurado com a média do **ano do modelo** do veículo, podemos confirmar hipóteses como: segurados mais jovens tendem a ter carros mais antigos? Ou segurados acima de 55 anos preferem veículos novos? Isso ajuda a entender o ciclo de vida e o poder de compra dos clientes.

**Consulta SQL:**
```sql
-- Pergunta 5: Relação entre ano do modelo e faixa etária (versão final)
SELECT
    i.descricao AS faixa_etaria,
    AVG(a.ano_modelo) AS media_ano_modelo
FROM
    arq_casco_comp a
JOIN
    auto_idade i ON a.faixa_etaria = i.codigo
WHERE
    i.codigo != 0
GROUP BY
    i.descricao
ORDER BY
    i.descricao;
```

---

## Questão 6: Análise de Prêmio

**Interpretação:**
Podemos detalhar a análise de prêmio por sexo, incluindo a **categoria do veículo**. Isso nos permite ver se a diferença de prêmio entre homens e mulheres é mais acentuada em "Pick-ups" do que em "Passeio Nacional", por exemplo. A análise fica mais granular e revela nuances importantes para a precificação.

**Consulta SQL:**
```sql
-- Pergunta 6: Média de prêmio por sexo e categoria do veículo (versão final)
SELECT
    s.descricao AS sexo,
    c.CATEGORIA AS categoria_veiculo,
    AVG(a.premio1) AS media_premio
FROM
    arq_casco_comp a
JOIN
    auto_sexo s ON a.sexo = s.codigo
JOIN
    auto_cat c ON a.cat_vei = c.CODIGO
WHERE
    s.codigo IN ('M', 'F')
GROUP BY
    s.descricao,
    c.CATEGORIA
ORDER BY
    s.descricao,
    c.CATEGORIA;
```

---

## Questão 7: Análise de Sinistros

**Interpretação:**
Além de analisar a frequência e o custo médio dos sinistros por região, podemos agora analisar a **causa** principal dos sinistros (roubo, colisão, etc.), usando a tabela `auto_cau`. Isso permite identificar os tipos de risco mais comuns e direcionar ações de prevenção.

**Consulta SQL:**
```sql
-- Pergunta 7.1: Total de sinistros por região (versão robusta)
SELECT
    regiao,
    descricao,
    SUM(numSinistros) AS total_sinistros
FROM
    SinReg
WHERE
    tipo_sin != 'TOTAL'
GROUP BY
    regiao,
    descricao
ORDER BY
    total_sinistros DESC;

-- Pergunta 7.2: Valor médio por indenização em cada região (versão robusta)
SELECT
    regiao,
    descricao,
    SUM(indenizacoes) / SUM(numSinistros) AS valor_medio_indenizacao
FROM
    SinReg
WHERE
    tipo_sin != 'TOTAL' AND numSinistros > 0
GROUP BY
    regiao,
    descricao
ORDER BY
    valor_medio_indenizacao DESC;

-- Pergunta 7.3: Análise de Causa do Sinistro (nova)
SELECT
    cau.CAUSA,
    COUNT(a4.id_apolice) as numero_de_eventos
FROM
    arq_casco4_comp a4
JOIN
    auto_cau cau ON a4.cau_sinistro = cau.CODIGO
GROUP BY
    cau.CAUSA
ORDER BY
    numero_de_eventos DESC;
```

---

## Questão 8: Interpretação dos Dados e Consulta Consolidada

**Interpretação Estratégica:**
A consulta consolidada final nos permite criar um perfil de cliente 360°. Podemos segmentar o mercado em nichos muito específicos, como: "Mulheres, entre 36-45 anos, com veículo de passeio importado, na região de Campinas". Para cada um desses nichos, temos o número de apólices, o prêmio médio e os dados de sinistralidade da região. Isso possibilita a criação de produtos altamente personalizados, marketing de precisão e uma política de preços muito mais justa e competitiva.

**Consulta SQL Consolidada:**
```sql
-- Pergunta 8: Consulta consolidada para criação de perfil (versão final e robusta)
WITH SinistrosPorRegiao AS (
    SELECT
        regiao,
        SUM(numSinistros) AS total_sinistros,
        SUM(indenizacoes) AS total_indenizacoes
    FROM
        SinReg
    WHERE
        tipo_sin != 'TOTAL'
    GROUP BY
        regiao
)
SELECT
    r.DESCRICAO AS regiao,
    sx.descricao AS sexo,
    i.descricao AS faixa_etaria,
    cat.CATEGORIA as categoria_veiculo,
    COUNT(a.id_apolice) AS numero_de_apolices,
    AVG(a.premio1) AS media_premio,
    sr.total_sinistros,
    CASE
        WHEN sr.total_sinistros > 0 THEN sr.total_indenizacoes / sr.total_sinistros
        ELSE 0
    END AS valor_medio_por_sinistro
FROM
    arq_casco_comp a
JOIN
    auto_reg r ON a.regiao = r.CODIGO
JOIN
    auto_sexo sx ON a.sexo = sx.codigo
JOIN
    auto_idade i ON a.faixa_etaria = i.codigo
JOIN
    auto_cat cat ON a.cat_vei = cat.CODIGO
LEFT JOIN
    SinistrosPorRegiao sr ON a.regiao = sr.regiao
WHERE
    sx.codigo IN ('M', 'F') AND i.codigo != 0
GROUP BY
    r.DESCRICAO,
    sx.descricao,
    i.descricao,
    cat.CATEGORIA,
    sr.total_sinistros,
    sr.total_indenizacoes
ORDER BY
    r.DESCRICAO,
    sx.descricao,
    i.descricao,
    cat.CATEGORIA;
```