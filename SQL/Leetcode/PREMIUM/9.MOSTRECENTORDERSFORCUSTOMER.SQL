-- fetch product name from products, product id from products, order_id from orders, order_date from orders
-- join the table customer_id and orders
-- join orders and products on product_id
-- partition the table by product_id and order by order_date desc
-- fetch where dense_rank is one
-- order by product_name asc, product_id asc, order_id asc
WITH CTE AS(
    SELECT
        P.PRODUCT_NAME AS PRODUCT_NAME,
        P.PRODUCT_ID AS PRODUCT_ID,
        O.ORDER_ID AS ORDER_ID,
        O.ORDER_DATE AS ORDER_DATE,
        dense_rank() OVER(
            PARTITION BY P.PRODUCT_ID
            ORDER BY
                ORDER_DATE DESC
        ) AS RNK
    FROM
        PRODUCTS AS P
        JOIN ORDERS AS O ON O.PRODUCT_ID = P.PRODUCT_ID
)
SELECT
    PRODUCT_NAME,
    product_id,
    ORDER_ID,
    ORDER_DATE
FROM
    CTE
WHERE
    RNK = 1
ORDER BY
    PRODUCT_NAME,
    PRODUCT_ID,
    ORDER_ID