SELECT
    NAME
FROM
    EMPLOYEE
WHERE
    ID IN(
        SELECT
            MANAGERID
        FROM
            EMPLOYEE
        GROUP BY
            MANAGERID
        HAVING
            COUNT(*) >= 5
    )