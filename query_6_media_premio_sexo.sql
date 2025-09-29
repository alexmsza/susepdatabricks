SELECT
    s.descricao AS Sexo,
    -- Conta o número de apólices para dar contexto à média
    COUNT(*) AS Quantidade_Apolices,
    -- Calcula a média do prêmio, arredondando para 2 casas decimais para melhor leitura
    ROUND(AVG(TRY_CAST(c.premio1 AS DECIMAL(18, 2))), 2) AS Media_Premio
FROM
    -- Tabela principal com os dados de apólices
    arq_casco_comp AS c
JOIN
    -- Tabela de domínio para decodificar o sexo
    auto_sexo AS s ON c.sexo = s.codigo
WHERE
    -- 1. Foca a análise apenas em segurados pessoa física (Masculino e Feminino)
    c.sexo IN ('M', 'F')
    -- 2. Garante que estamos considerando apenas prêmios válidos e maiores que zero
    AND TRY_CAST(c.premio1 AS DECIMAL(18, 2)) > 0
GROUP BY
    s.descricao
ORDER BY
    -- Ordena o resultado pela média de prêmio, do maior para o menor
    Media_Premio DESC;