SELECT
    WORKER_TITLE
FROM
    (
        SELECT
            *,
            DENSE_RANK() OVER(
                ORDER BY
                    SALARY DESC
            ) AS RNK
        FROM
            WORKER AS W
            JOIN TITLE AS T ON W.WORKER_ID = T.WORKER_REF_ID
    ) AS SUB_QUERY
WHERE
    RNK = 1
ORDER BY
    WORKER_TITLE ASC