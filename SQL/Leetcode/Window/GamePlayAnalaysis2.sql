WITH CTE AS(
    SELECT
        *,
        ROW_NUMBER() OVER(
            PARTITION BY PLAYER_ID
            ORDER BY
                EVENT_DATE
        ) AS RNK
    FROM
        ACTIVITY
)
SELECT
    PLAYER_ID,
    DEVICE_ID
FROM
    CTE
WHERE
    RNK = 1