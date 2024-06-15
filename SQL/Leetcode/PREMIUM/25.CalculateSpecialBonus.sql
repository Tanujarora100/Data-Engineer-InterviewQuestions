SELECT
    EMPLOYEE_ID,
    CASE WHEN (
        (
            MOD(EMPLOYEE_ID, 2) != 0
            AND LEFT(NAME, 1) != 'M'
        )
    ) THEN SALARY ELSE 0 END AS BONUS
FROM
    Employees
ORDER BY
    EMPLOYEE_ID