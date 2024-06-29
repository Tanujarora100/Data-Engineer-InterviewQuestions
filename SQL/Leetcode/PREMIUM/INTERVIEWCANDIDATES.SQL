SELECT
    CANDIDATE_ID
FROM
    (
        SELECT
            CANDIDATE_ID,
            SUM(SCORE) AS INTERVIEW_SCORE
        FROM
            (
                SELECT
                    C.*,
                    R.SCORE
                FROM
                    CANDIDATES AS C
                    JOIN ROUNDS AS R ON C.INTERVIEW_ID = R.INTERVIEW_ID
                WHERE
                    C.YEARS_OF_EXP >= 2
            ) AS SUB_QUERY
        GROUP BY
            CANDIDATE_ID
    ) AS NESTED_QUERY
WHERE
    INTERVIEW_SCORE > 15