WITH CTE AS(
    SELECT
        E1.EMPLOYEE_ID,
        E1.AGE,
        E1.NAME AS EMPLOYEE_NAME,
        E1.REPORTS_TO AS MANAGER_ID,
        E2.NAME AS MANAGER_NAME
    FROM
        employees e1
        LEFT JOIN employees e2 ON e1.reports_to = e2.employee_id
)
SELECT
    MANAGER_ID AS employee_id,
    MANAGER_NAME AS name,
    COUNT(EMPLOYEE_NAME) AS reports_count,
    ROUND(AVG(AGE)) AS average_age
FROM
    CTE
GROUP BY
    employee_id,
    name
HAVING
    reports_count > 0
    AND name IS NOT NULL
ORDER BY
    employee_id