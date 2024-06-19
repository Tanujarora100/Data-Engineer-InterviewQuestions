SELECT
    STUDENT_ID,
    HRS_STUDIED,
    SAT_MATH,
    AVG(SAT_MATH / HRS_STUDIED) AS points_per_hour
FROM
    SAT_SCORES
WHERE
    HRS_STUDIED > 1
GROUP BY
    1,
    2,
    3
ORDER BY
    points_per_hour DESC
LIMIT
    1