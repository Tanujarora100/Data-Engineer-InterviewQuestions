SELECT
    DISTINCT NUM AS ConsecutiveNums
FROM
    (
        SELECT
            ID,
            NUM,
            LAG(NUM) OVER(
                ORDER BY
                    ID
            ) AS PREVIOUS_NUMBER,
            LEAD(NUM) OVER(
                ORDER BY
                    ID
            ) AS NEXT_NUMBER
        FROM
            LOGS
    ) AS SUB_QUERY
WHERE
    NUM = PREVIOUS_NUMBER
    AND NUM = NEXT_NUMBERX