-- Pergunta 5: Relação entre ano do modelo e faixa etária (versão final)
-- Mostra a média do ano do modelo para cada faixa etária de segurado.
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