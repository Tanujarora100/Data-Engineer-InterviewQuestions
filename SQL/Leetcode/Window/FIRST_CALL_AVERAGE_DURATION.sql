WITH CTE AS(
    SELECT
        *,
        ROW_NUMBER() OVER(
            PARTITION BY REQUEST_ID
            ORDER BY
                ID
        ) AS TOTAL_CALLS
    FROM
        redfin_call_tracking
)
SELECT
    AVG(CALL_DURATION) AS FIRST_CALL_AVERAGE_DURATION
FROM
    CTE
WHERE
    TOTAL_CALLS = 1;