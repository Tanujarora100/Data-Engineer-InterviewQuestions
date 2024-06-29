WITH CTE AS (
    SELECT
        SEAT_ID,
        LAG(SEAT_ID, 1) OVER(
            ORDER BY
                SEAT_ID
        ) AS PREVIOUS_SEAT_NUMBER,
        LEAD(SEAT_ID, 1) OVER(
            ORDER BY
                SEAT_ID
        ) AS NEXT_SEAT_NUMBER
    FROM
        (
            SELECT
                SEAT_ID
            FROM
                CINEMA
            WHERE
                FREE = 1
        ) AS A
)
SELECT
    DISTINCT SEAT_ID
FROM
    CTE
WHERE
    SEAT_ID = NEXT_SEAT_NUMBER -1
    OR SEAT_ID = PREVIOUS_SEAT_NUMBER + 1