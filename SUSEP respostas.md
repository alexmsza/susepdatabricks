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

4. Análise por Região:
   - Questão: Escreva uma consulta SQL para calcular a média de idade por região no arquivo `arq_casco_comp`. O que esses resultados podem indicar sobre a distribuição demográfica dos segurados por região?
   - Objetivo: Aplicar funções agregadas com agrupamento e incentivar a interpretação de dados regionais.

5. Análise por Ano do Modelo:
   - Questão: Como você consultaria a média de idade por ano do modelo no arquivo `arq_casco_comp`? Que conclusões podem ser tiradas sobre a relação entre a idade dos segurados e o ano do modelo dos veículos?
   - Objetivo: Fazer a conexão entre diferentes dimensões de dados e seus impactos.

6. Análise de Prêmio:
   - Questão: Utilize SQL para calcular a média de `premio1` por sexo no arquivo `arq_casco_comp`. Como você interpretaria as diferenças encontradas entre os sexos?
   - Objetivo: Explorar as disparidades de prêmios de seguro entre diferentes grupos.

7. Análise de Sinistros:
   - Questão: Qual seria a consulta SQL para descobrir a média de sinistros (`numSinistros`) por região no arquivo `SinReg`? O que esses dados revelam sobre a frequência de sinistros em diferentes regiões?
   - Questão: Qual a média de indenizações levando em consideração o valor das indenizações (`indenizacoes`) e o número de sinistros (`numSinistros`)? 
   - Objetivo: Analisar padrões de sinistralidade geográfica.

8. Interpretação dos Dados:
   - Questão: Após realizar as análises pedidas (idade, prêmio, IS_MEDIA), como você correlacionaria esses resultados para criar um perfil detalhado dos segurados por região, sexo e ano do modelo? Quais insights estratégicos você pode extrair desses dados?
   - Objetivo: Desenvolver a habilidade de correlacionar e interpretar múltiplas dimensões de dados para gerar insights mais profundos.
