WITH CTE AS (
    SELECT
        phone_number,
        start_time,
        ROW_NUMBER() OVER(
            PARTITION BY phone_number
            ORDER BY
                start_time
        ) AS RNK
    FROM
        CALL_START_LOGS
),
ENDING_LOGS AS (
    SELECT
        phone_number,
        end_time,
        ROW_NUMBER() OVER(
            PARTITION BY phone_number
            ORDER BY
                end_time
        ) AS RNK
    FROM
        CALL_END_LOGS
)
SELECT
    c.phone_number,
    c.start_time,
    e.end_time,
    abs(
        extract(
            minute
            from
                c.start_time
        ) - extract(
            minute
            from
                e.end_time
        )
    ) as total_duration
FROM
    CTE AS c
    JOIN ENDING_LOGS AS e ON c.phone_number = e.phone_number
    AND c.RNK = e.RNK;