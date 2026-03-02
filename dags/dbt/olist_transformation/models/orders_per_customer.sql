SELECT 
    c.customer_unique_id,
    c.customer_city,
    COALESCE(COUNT(o.order_id)) as qty_orders
FROM {{ source('public', 'customers') }} c
JOIN {{ source('public', 'orders') }} o ON c.customer_id = o.customer_id
GROUP BY 1, 2
ORDER BY qty_orders DESC    