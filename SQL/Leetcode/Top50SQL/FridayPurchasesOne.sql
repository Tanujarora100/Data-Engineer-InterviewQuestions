SELECT
    FLOOR((DAYOFMONTH(purchase_date)) / 7) + 1 AS week_of_month,
    purchase_date,
    SUM(amount_spend) AS total_amount
FROM
    purchases
WHERE
    YEAR(purchase_date) = 2023
    AND MONTH(purchase_date) = 11
    AND DAYNAME(purchase_date) = 'Friday'
GROUP BY
    purchase_date,
    FLOOR((DAYOFMONTH(purchase_date)) / 7) + 1
ORDER BY
    purchase_date;