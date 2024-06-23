SELECT
    EMPLOYEE_NAME,
    EMP_SALARY
FROM
    (
        SELECT
            E1.FIRST_NAME AS EMPLOYEE_NAME,
            E1.SALARY AS EMP_SALARY,
            E1.MANAGER_ID,
            E2.FIRST_NAME AS MANAGER_NAME,
            E2.SALARY AS MANAGER_SALARY
        FROM
            employee AS E1
            JOIN employee AS E2 ON E1.MANAGER_ID = E2.ID
        WHERE
            E1.SALARY > E2.SALARY
    ) AS SUB_QUERY