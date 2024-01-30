with

sales as (
    select * from {{ ref("stg_raw__sales")}}
),

product as (
    select * from {{ ref("stg_raw__product")}}
)

select
    products_id,
    date_date,
    orders_id,
    revenue,
    quantity,
    purchase_price,
    --CAST(purchase_price AS FLOAT64)
    ROUND(sales.quantity*CAST(product.purchase_price AS FLOAT64),2) AS purchase_cost,
    sales.revenue - ROUND(sales.quantity*CAST(product.purchase_price AS FLOAT64),2) AS margin
from sales
left join product using (products_id)

