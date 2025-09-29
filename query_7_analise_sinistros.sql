SELECT
    descricao AS Regiao,
    -- Converte a coluna de sinistros para um número, tratando possíveis erros
    TRY_CAST(numSinistros AS BIGINT) AS Total_Sinistros
FROM
    SinReg
WHERE
    -- Filtramos para obter apenas a linha que já contém o total de sinistros da região
    tipo_sin = 'TOTAL'
    AND TRY_CAST(numSinistros AS BIGINT) IS NOT NULL
ORDER BY
    -- Ordenamos da região com maior número de sinistros para a menor
    Total_Sinistros DESC;