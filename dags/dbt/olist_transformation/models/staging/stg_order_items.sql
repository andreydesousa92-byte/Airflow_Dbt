SELECT
    order_id,
    order_item_id,
    product_id,
    seller_id,
    shipping_limit_date:: TIMESTAMP AS shipping_limit_at,
    price:: NUMERIC,
    freight_value:: NUMERIC
FROM {{ source('public', 'order_items')}}