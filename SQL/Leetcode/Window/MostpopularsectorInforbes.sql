SELECT
    SECTOR,
    N_COMPANIES
FROM
    (
        SELECT
            SECTOR,
            COUNT(1) AS N_COMPANIES,
            DENSE_RANK() OVER(
                ORDER BY
                    COUNT(1) DESC
            ) AS RNK
        FROM
            forbes_global_2010_2014
        GROUP BY
            SECTOR
    ) AS SUB_QUERY
WHERE
    RNK = 1