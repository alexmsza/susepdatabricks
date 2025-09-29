%sql
-- Insight 1: Contagem de segurados por sexo
-- Esta consulta agrupa os segurados por sexo e conta o número de apólices para cada um.
SELECT
    sexo,
    COUNT(*) AS total_apolices -- CORREÇÃO AQUI
FROM
    arq_casco_comp
GROUP BY
    sexo;

-- Insight 2: Total de sinistros por tipo de cobertura
-- Esta consulta soma o número de sinistros para cada tipo de cobertura (CASCO, RCDM, etc.).
SELECT
    tipo_sin,
    SUM(numSinistros) AS total_sinistros
FROM
    SinReg
WHERE
    tipo_sin != 'TOTAL' -- Exclui a linha de total para evitar contagem dupla
GROUP BY
    tipo_sin
ORDER BY
    total_sinistros DESC;

-- Insight 3: Total de sinistros por região
-- Soma o número de sinistros por região para identificar as áreas de maior ocorrência.
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
