WITH CTE AS(
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY USER_ID
            ORDER BY
                WATCHED_AT
        ) AS VIDEO_WATCH_RECORD
    FROM
        VIDEOS_WATCHED
),
RANKED_CTE AS(
    SELECT
        VIDEO_ID,
        COUNT(*) AS N_VIDEOS_IN_3,
        DENSE_RANK() OVER(
            ORDER BY
                COUNT(*) DESC
        ) AS RNK
    FROM
        CTE
    WHERE
        VIDEO_WATCH_RECORD <= 3
    GROUP BY
        VIDEO_ID
)
SELECT
    VIDEO_ID,
    N_VIDEOS_IN_3
FROM
    RANKED_CTE
WHERE
    RNK <= 3;