select
    p.prod_sku_id,
    p.market_name
from
    dim_product as p full
    outer join fct_customer_sales on p.prod_sku_id = fct_customer_sales.prod_sku_id
where
    fct_customer_sales.order_id is Null