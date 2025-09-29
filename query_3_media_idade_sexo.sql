%sql
-- Pergunta 3: Distribuição de segurados por faixa etária e sexo (PROVÁVEL VERSÃO CORRIGIDA)
-- A coluna 'faixa_etaria' na tabela 'arq_casco_comp' foi substituída por 'IDADE'.
SELECT
    i.descricao AS faixa_etaria,
    s.descricao AS sexo,
    COUNT(*) AS numero_de_apolices -- Mantenho COUNT(*) para evitar erro em 'id_apolice'
FROM
    arq_casco_comp a
JOIN
    auto_idade i ON a.IDADE = i.codigo -- CORREÇÃO MAIS PROVÁVEL: a.IDADE em vez de a.faixa_etaria
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