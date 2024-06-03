WITH CTE AS(
    SELECT
        E.SALARY,
        O.DEPARTMENT
    FROM
        db_employee AS E
        JOIN db_dept AS O ON O.ID = E.DEPARTMENT_ID
    WHERE
        O.DEPARTMENT IN ('marketing', 'engineering')
),
MARKETING_CTE AS(
    SELECT
        MAX(SALARY) AS marketing_max_salary
    FROM
        CTE
    WHERE
        DEPARTMENT = 'marketing'
),
engg_cte AS(
    SELECT
        MAX(SALARY) AS engineering_max_salary
    FROM
        CTE
    WHERE
        DEPARTMENT = 'engineering'
)
SELECT
    marketing_max_salary - engineering_max_salary AS salary_difference
FROM
    MARKETING_CTE,
    engg_cte;
    /* Solution 2 and less repetition  */
    WITH CTE AS(
        SELECT
            E.SALARY,
            O.DEPARTMENT
        FROM
            db_employee AS E
            JOIN db_dept AS O ON O.ID = E.DEPARTMENT_ID
        WHERE
            O.DEPARTMENT IN ('marketing', 'engineering')
    )
SELECT
    MAX(CASE WHEN DEPARTMENT = 'marketing' THEN SALARY END) - MAX(
        CASE WHEN DEPARTMENT = 'engineering' THEN SALARY END
    ) AS SALARY_DIFFERENCE
FROM
    CTE;