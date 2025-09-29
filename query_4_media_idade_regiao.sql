-- Pergunta 4: Distribuição de segurados por faixa etária e região (versão final)
-- Conta o número de apólices para cada faixa etária em cada região.
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
