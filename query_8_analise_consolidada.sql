-- Pergunta 8: Consulta consolidada para criação de perfil (versão final e robusta)
-- Cria um perfil de segurado detalhado, unindo região, sexo, faixa etária e categoria do veículo.
-- A seção de sinistros agora soma as partes em vez de usar a linha TOTAL.
WITH SinistrosPorRegiao AS (
    SELECT
        regiao,
        SUM(numSinistros) AS total_sinistros,
        SUM(indenizacoes) AS total_indenizacoes
    FROM
        SinReg
    WHERE
        tipo_sin != 'TOTAL' -- Soma as partes para construir o total
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