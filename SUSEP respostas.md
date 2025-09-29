1. Importação e Preparação de Dados:
   - Questão: Como você importaria os arquivos CSV do 2º semestre de 2020 para uma plataforma como o Databricks usando SQL? Descreva os passos necessários para garantir que os dados sejam carregados corretamente e estejam prontos para análise.
   - Objetivo: Avaliar a compreensão sobre importação e preparação de dados.
   - Resposta: Usando um script para fazer a importação do arquivo zip direto para o volume e extrai-lo dentro do volume.

```python
import requests
import zipfile
import io
import os

# URL of the ZIaP file
url = "https://www2.susep.gov.br/redarq.asp?arq=Autoseg2021A%2ezip"

# Download the ZIP file without verifying the certificate
response = requests.get(url, verify=False)
response.raise_for_status()

# Local directory for extracted files
output_dir = "/Volumes/workspace/default/susep-data/autoseg2021/raw"
os.makedirs(output_dir, exist_ok=True)

# Extract the ZIP contents
with zipfile.ZipFile(io.BytesIO(response.content)) as z:
    z.printdir()
    z.extractall(output_dir)

print(f"Folder created at: {output_dir}")
```

2. Exploração de Dados:
   - Questão: Quais são os primeiros insights que você pode extrair ao explorar as bases de dados importadas? Cite ao menos três descobertas iniciais baseadas em consultas SQL simples.
   - Objetivo: Incentivar a exploração inicial dos dados para identificar padrões ou anomalias.
   - Resposta:
   1. Mulheres possuem mais apólices que homens.

```sql
SELECT
    sexo,
    COUNT(*) AS total_apolices -- CORREÇÃO AQUI
FROM
    arq_casco_comp
GROUP BY
    sexo;
```
    
2. O sinistro com maior tipo de cobertura é o CASCO.

```sql
SELECT
    tipo_sin,
    SUM(numSinistros) AS total_sinistros
FROM
    SinReg
WHERE
    tipo_sin != 'TOTAL'
GROUP BY
    tipo_sin
ORDER BY
    total_sinistros DESC;
```

3. A região com mais sinistros é a região metropolitana de São Paulo.
```sql
SELECT
    regiao,
    descricao,
    SUM(numSinistros) AS total_sinistros
FROM
    SinReg
WHERE
    tipo_sin = 'TOTAL'
GROUP BY
    regiao,
    descricao
ORDER BY
    total_sinistros DESC;
```


3. Análise por Sexo:
   - Questão: Usando SQL, como você calcularia a média de idade dos segurados por sexo no arquivo `arq_casco_comp`? Qual é a interpretação desses resultados em termos de perfil dos segurados?
   - Objetivo: Ensinar a aplicação de funções agregadas e a análise dos resultados.
   - Resposta: O grupo acima de 55 anos é o que mais tem seguros.
```sql
    i.descricao AS faixa_etaria,
    s.descricao AS sexo,
    COUNT(*) AS numero_de_apolices 
FROM
    arq_casco_comp a
JOIN
    auto_idade i ON a.IDADE = i.codigo 
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

4. Análise por Região:
   - Questão: Escreva uma consulta SQL para calcular a média de idade por região no arquivo `arq_casco_comp`. O que esses resultados podem indicar sobre a distribuição demográfica dos segurados por região?
   - Objetivo: Aplicar funções agregadas com agrupamento e incentivar a interpretação de dados regionais.
   - Resposta: A média das regiõe se concentra entre 44 e 45.
```sql
SELECT
    r.DESCRICAO AS Regiao,
    AVG(
        CASE
            WHEN TRY_CAST(c.IDADE AS BIGINT) = 1 THEN 21.5 
            WHEN TRY_CAST(c.IDADE AS BIGINT) = 2 THEN 30.5
            WHEN TRY_CAST(c.IDADE AS BIGINT) = 3 THEN 40.5
            WHEN TRY_CAST(c.IDADE AS BIGINT) = 4 THEN 50.5
            WHEN TRY_CAST(c.IDADE AS BIGINT) = 5 THEN 60.0
        END
    ) AS Media_Idade_Aproximada
FROM
    arq_casco_comp AS c
JOIN
    auto_reg AS r
    ON TRY_CAST(c.REGIAO AS BIGINT) = TRY_CAST(r.CODIGO AS BIGINT)
WHERE
    TRY_CAST(c.IDADE AS BIGINT) <> 0
    AND TRY_CAST(c.IDADE AS BIGINT) IS NOT NULL
GROUP BY
    r.DESCRICAO
ORDER BY
    Regiao;
```
5. Análise por Ano do Modelo:
   - Questão: Como você consultaria a média de idade por ano do modelo no arquivo `arq_casco_comp`? Que conclusões podem ser tiradas sobre a relação entre a idade dos segurados e o ano do modelo dos veículos?
   - Objetivo: Fazer a conexão entre diferentes dimensões de dados e seus impactos.
   - Resposta: Quanto mais antigo o veículo maior a média de idade do indivíduo e quanto mais recente o veículo menor é a média de idade.

```sql
SELECT
    TRY_CAST(c.ANO_MODELO AS BIGINT) AS Ano_Modelo,
    COUNT(*) AS Quantidade_Veiculos,
    AVG(
        CASE
            WHEN TRY_CAST(c.IDADE AS BIGINT) = 1 THEN 21.5 -- Ponto médio para 'Entre 18 e 25 anos'
            WHEN TRY_CAST(c.IDADE AS BIGINT) = 2 THEN 30.5 -- Ponto médio para 'Entre 26 e 35 anos'
            WHEN TRY_CAST(c.IDADE AS BIGINT) = 3 THEN 40.5 -- Ponto médio para 'Entre 36 e 45 anos'
            WHEN TRY_CAST(c.IDADE AS BIGINT) = 4 THEN 50.5 -- Ponto médio para 'Entre 46 e 55 anos'
            WHEN TRY_CAST(c.IDADE AS BIGINT) = 5 THEN 60.0 -- Valor estimado para 'Maior que 55 anos'
        END
    ) AS Media_Idade_Aproximada
FROM
    arq_casco_comp AS c
WHERE
    TRY_CAST(c.ANO_MODELO AS BIGINT) > 1980
    AND TRY_CAST(c.IDADE AS BIGINT) > 0
GROUP BY
    TRY_CAST(c.ANO_MODELO AS BIGINT)
ORDER BY
    Ano_Modelo DESC;
```

6. Análise de Prêmio:
   - Questão: Utilize SQL para calcular a média de `premio1` por sexo no arquivo `arq_casco_comp`. Como você interpretaria as diferenças encontradas entre os sexos?
   - Objetivo: Explorar as disparidades de prêmios de seguro entre diferentes grupos.
   - Respostas: Pode se perceber que a média de premios apresentar um valor maior para homens.

```sql
SELECT
    s.descricao AS Sexo,
    COUNT(*) AS Quantidade_Apolices,
    ROUND(AVG(TRY_CAST(c.premio1 AS DECIMAL(18, 2))), 2) AS Media_Premio
FROM
    arq_casco_comp AS c
JOIN
    auto_sexo AS s ON c.sexo = s.codigo
WHERE
    c.sexo IN ('M', 'F')
    AND TRY_CAST(c.premio1 AS DECIMAL(18, 2)) > 0
GROUP BY
    s.descricao
ORDER BY
    Media_Premio DESC;
```

7. Análise de Sinistros:
   - Questão: Qual seria a consulta SQL para descobrir a média de sinistros (`numSinistros`) por região no arquivo `SinReg`? O que esses dados revelam sobre a frequência de sinistros em diferentes regiões?
   - Questão: Qual a média de indenizações levando em consideração o valor das indenizações (`indenizacoes`) e o número de sinistros (`numSinistros`)? 
   - Objetivo: Analisar padrões de sinistralidade geográfica.
   - Respostas: A méida de sinistros é maior na região metropolitana de São Paulo e menor em Roraima.
```sql
SELECT
    descricao AS Regiao,
    TRY_CAST(numSinistros AS BIGINT) AS Total_Sinistros
FROM
    SinReg
WHERE
    tipo_sin = 'TOTAL'
    AND TRY_CAST(numSinistros AS BIGINT) IS NOT NULL
ORDER BY
    Total_Sinistros DESC;
```

8. Interpretação dos Dados:
   - Questão: Após realizar as análises pedidas (idade, prêmio, IS_MEDIA), como você correlacionaria esses resultados para criar um perfil detalhado dos segurados por região, sexo e ano do modelo? Quais insights estratégicos você pode extrair desses dados?
   - Objetivo: Desenvolver a habilidade de correlacionar e interpretar múltiplas dimensões de dados para gerar insights mais profundos.
   - Resposta:  O maior número de clientes em composto por mulheres. As regiões metropolintas e principalmente a de São Paulo concentra um maior número de sinistros. Homens jovens tem um valor de prêmio mais alto