SELECT
    r.DESCRICAO AS Regiao,
    -- Calcula a média dos valores numéricos convertidos
    AVG(
        CASE
            WHEN TRY_CAST(c.IDADE AS BIGINT) = 1 THEN 21.5 -- Ponto médio para 'Entre 18 e 25 anos'
            WHEN TRY_CAST(c.IDADE AS BIGINT) = 2 THEN 30.5 -- Ponto médio para 'Entre 26 e 35 anos'
            WHEN TRY_CAST(c.IDADE AS BIGINT) = 3 THEN 40.5 -- Ponto médio para 'Entre 36 e 45 anos'
            WHEN TRY_CAST(c.IDADE AS BIGINT) = 4 THEN 50.5 -- Ponto médio para 'Entre 46 e 55 anos'
            WHEN TRY_CAST(c.IDADE AS BIGINT) = 5 THEN 60.0 -- Valor estimado para 'Maior que 55 anos'
            -- Se o valor for nulo ou inválido, ele não entra no cálculo da média
        END
    ) AS Media_Idade_Aproximada
FROM
    -- Tabela principal, apelidada de 'c'
    arq_casco_comp AS c
JOIN
    -- Tabela de regiões, apelidada de 'r'
    auto_reg AS r
    -- A junção é feita com TRY_CAST para ignorar valores inválidos em ambas as colunas
    ON TRY_CAST(c.REGIAO AS BIGINT) = TRY_CAST(r.CODIGO AS BIGINT)
WHERE
    -- Filtra registros onde a idade é '0' (Não informada) ou inválida (NULL)
    TRY_CAST(c.IDADE AS BIGINT) <> 0
    AND TRY_CAST(c.IDADE AS BIGINT) IS NOT NULL
GROUP BY
    -- Agrupa os resultados por nome da região para que o AVG funcione corretamente
    r.DESCRICAO
ORDER BY
    -- Ordena o resultado final em ordem alfabética
    Regiao;