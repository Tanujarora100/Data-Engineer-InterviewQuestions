-- Common Table Expression to get the department with the largest number of employees
WITH CTE AS (
    SELECT
        department_id,
        COUNT(DISTINCT id) AS total_employees,
        RANK() OVER (
            ORDER BY
                COUNT(DISTINCT id) DESC
        ) AS rnk
    FROM
        az_employees
    GROUP BY
        department_id
)
SELECT
    e.first_name,
    e.last_name
FROM
    az_employees e
    JOIN CTE c ON e.department_id = c.department_id
WHERE
    c.rnk = 1
    AND (
        e.position iLike '% Manager'
        or e.position ilike 'Manager%'
    );