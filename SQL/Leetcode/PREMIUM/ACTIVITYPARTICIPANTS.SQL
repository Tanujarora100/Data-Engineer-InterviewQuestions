WITH cte AS (
    SELECT
        activity,
        DENSE_RANK() OVER (
            ORDER BY
                COUNT(DISTINCT id) DESC
        ) AS total_counts
    FROM
        friends
    GROUP BY
        activity
)
SELECT
    ACTIVITY
FROM
    CTE
WHERE
    TOTAL_COUNTS > (
        SELECT
            MIN(TOTAL_COUNTS)
        FROM
            CTE
    )
    AND TOTAL_COUNTS <(
        SELECT
            MAX(TOTAL_COUNTS)
        FROM
            CTE
    )