SELECT
    c.customer_unique_id,
    MAX(c.city)                                         AS city,
    MAX(c.state)                                        AS state,
    COUNT(o.order_id)                                   AS qty_orders,
    SUM(o.is_delivered::INT)                            AS qty_delivered,
    ROUND(SUM(o.is_delivered::INT) * 100.0 
        / NULLIF(COUNT(o.order_id), 0), 2)              AS pct_delivered
FROM {{ ref('stg_customers') }} c
LEFT JOIN {{ ref('stg_orders') }} o 
    ON c.customer_id = o.customer_id
GROUP BY 1