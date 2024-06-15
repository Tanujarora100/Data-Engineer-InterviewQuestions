SELECT
    (
        CASE WHEN E.name IS NULL THEN S.employee_id WHEN S.salary IS NULL THEN E.employee_id END
    ) employee_id
FROM
    Employees E FULL
    JOIN Salaries S on E.employee_id = S.employee_id
WHERE
    E.name IS NULL
    OR S.salary IS NULL
ORDER BY
    employee_id;