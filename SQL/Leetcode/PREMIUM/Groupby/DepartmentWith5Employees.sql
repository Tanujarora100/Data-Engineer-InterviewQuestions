SELECT
    DEPARTMENT
FROM
    (
        SELECT
            DEPARTMENT,
            COUNT(ID) AS TOTAL_EMP
        FROM
            EMPLOYEE
        GROUP BY
            DEPARTMENT
        HAVING
            COUNT(ID) >= 5
    ) AS A