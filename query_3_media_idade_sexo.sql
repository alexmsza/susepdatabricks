-- Pergunta 3: Distribuição de segurados por faixa etária e sexo (versão final)
-- Conta o número de apólices para cada faixa etária e sexo.
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
