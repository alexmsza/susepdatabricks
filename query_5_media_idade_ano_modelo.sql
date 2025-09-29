SELECT
    -- Seleciona o ano do modelo, garantindo que seja um número válido
    TRY_CAST(c.ANO_MODELO AS BIGINT) AS Ano_Modelo,
    -- Conta quantos veículos existem para aquele ano na amostra
    COUNT(*) AS Quantidade_Veiculos,
    -- Calcula a idade média aproximada para cada ano de modelo
    AVG(
        CASE
            WHEN TRY_CAST(c.IDADE AS BIGINT) = 1 THEN 21.5 -- Ponto médio para 'Entre 18 e 25 anos'
            WHEN TRY_CAST(c.IDADE AS BIGINT) = 2 THEN 30.5 -- Ponto médio para 'Entre 26 e 35 anos'
            WHEN TRY_CAST(c.IDADE AS BIGINT) = 3 THEN 40.5 -- Ponto médio para 'Entre 36 e 45 anos'
            WHEN TRY_CAST(c.IDADE AS BIGINT) = 4 THEN 50.5 -- Ponto médio para 'Entre 46 e 55 anos'
            WHEN TRY_CAST(c.IDADE AS BIGINT) = 5 THEN 60.0 -- Valor estimado para 'Maior que 55 anos'
        END
    ) AS Media_Idade_Aproximada
FROM
    arq_casco_comp AS c
WHERE
    -- Filtra linhas onde o ano do modelo é inválido ou muito antigo (ex: antes de 1980)
    TRY_CAST(c.ANO_MODELO AS BIGINT) > 1980
    -- Filtra linhas onde a idade é inválida ou não informada (código 0)
    AND TRY_CAST(c.IDADE AS BIGINT) > 0
GROUP BY
    -- Agrupa todos os cálculos pelo ano do modelo
    TRY_CAST(c.ANO_MODELO AS BIGINT)
ORDER BY
    -- Ordena do ano mais recente para o mais antigo, para facilitar a leitura da tendência
    Ano_Modelo DESC;