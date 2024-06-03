WITH CTE AS (
    SELECT
        DISTINCT HOTEL_NAME,
        AVG(AVERAGE_SCORE) AS AVERAGE_SCORE,
    FROM
        hotel_reviews
),
RANKED_CTE AS(
    SELECT
        HOTEL_NAME,
        AVERAGE_SCORE,
        RANK() OVER (
            ORDER BY
                AVERAGE_SCORE DESC
        ) AS RNK
    FROM
        CTE
)
SELECT
    HOTEL_NAME,
    AVERAGE_SCORE
FROM
    RANKED_CTE
WHERE
    RNK <= 10