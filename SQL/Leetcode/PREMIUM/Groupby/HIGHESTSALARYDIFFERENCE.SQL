SELECT
    ABS(
        (
            SELECT
                MAX(SALARY)
            FROM
                SALARIES
            WHERE
                DEPARTMENT = 'Marketing'
        ) -(
            SELECT
                MAX(SALARY)
            FROM
                SALARIES
            WHERE
                DEPARTMENT = 'Engineering'
        )
    ) as SALARY_DIFFERENCE