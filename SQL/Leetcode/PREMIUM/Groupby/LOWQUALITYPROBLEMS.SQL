SELECT
    PROBLEM_ID
FROM
    PROBLEMS
WHERE
    ((LIKES / (LIKES + DISLIKES)) * 100 < 60)
ORDER BY
    PROBLEM_ID ASC