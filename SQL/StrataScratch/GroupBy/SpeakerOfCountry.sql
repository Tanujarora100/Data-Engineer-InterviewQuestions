WITH cte AS(
    SELECT
        p.user_id,
        p.location,
        u.language,
        p.event_type
    FROM
        playbook_events AS p
        INNER JOIN playbook_users AS u ON p.user_id = u.user_id
)
SELECT
    location,
    COUNT(DISTINCT user_id) AS n_logins
FROM
    cte
WHERE
    language = 'spanish'
GROUP BY
    location
ORDER BY
    COUNT(DISTINCT user_id) DESC;