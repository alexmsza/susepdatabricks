-- Pergunta 6: Média de prêmio por sexo e categoria do veículo (versão final)
-- Analisa o prêmio médio pago por sexo, quebrando por categoria do veículo.
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
