WITH CTE AS(
    SELECT
        BIKE_NUMBER,
        END_TIME,
        ROW_NUMBER() OVER(
            PARTITION BY BIKE_NUMBER
            ORDER BY
                END_TIME DESC
        ) AS RNK
    FROM
        BIKES
)
SELECT
    BIKE_NUMBER,
    END_TIME
FROM
    CTE
WHERE
    RNK = 1
ORDER BY
    END_TIME DESC