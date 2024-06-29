WITH CTE AS (
    SELECT
        CITY,
        EXTRACT(
            HOUR
            FROM
                CALL_TIME
        ) AS CALLING_HOUR,
        COUNT(*),
        DENSE_RANK() OVER(
            PARTITION BY CITY
            ORDER BY
                COUNT(*) DESC
        ) AS RNK
    FROM
        CALLS
    GROUP BY
        CITY,
        CALLING_HOUR
)
SELECT
    CITY,
    CALLING_HOUR AS PEAK_CALLING_HOUR,
    COUNT AS NUMBER_OF_CALLS
FROM
    CTE
WHERE
    RNK = 1
ORDER BY
    PEAK_CALLING_HOUR DESC,
    CITY DESC