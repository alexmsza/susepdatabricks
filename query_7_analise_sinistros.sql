-- Pergunta 7.1: Total de sinistros por região (versão robusta)
-- Soma o número de sinistros de cada tipo (exceto o TOTAL) para construir um total preciso por região.
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
-- Calcula o custo médio por sinistro somando os valores e quantidades de cada tipo de sinistro.
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

-- Pergunta 7.3: Análise de Causa do Sinistro (inalterada)
-- Esta consulta usa arq_casco4_comp, que contém detalhes do sinistro.
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
