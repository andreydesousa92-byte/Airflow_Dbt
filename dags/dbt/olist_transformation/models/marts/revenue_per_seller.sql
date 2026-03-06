SELECT
    s.seller_id,
    s.city,
    s.state,
    COUNT(DISTINCT oi.order_id) AS qty_orders,
    COUNT(oi.order_item_id) AS qty_items_sold,

    ROUND(SUM(oi.price)::NUMERIC, 2) AS total_revenue,
    ROUND(AVG(oi.price)::NUMERIC, 2) AS avg_ticket,
    ROUND(SUM(oi.freight_value)::NUMERIC, 2) AS total_freight,

    ROUND(100.0 * SUM(
        CASE WHEN o.is_delivered = true THEN 1 ELSE 0 END
    ) / COUNT(DISTINCT oi.order_id), 1) AS pct_delivered,

    COALESCE(ROUND(AVG(
        EXTRACT(EPOCH FROM (o.delivered_customer_at - o.purchased_at)) / 86400
    )::NUMERIC, 1), 0) AS avg_days_to_deliver

FROM {{ ref('stg_order_items') }} oi
JOIN {{ ref('stg_orders') }} o       ON oi.order_id = o.order_id
JOIN {{ ref('stg_sellers') }} s      ON oi.seller_id = s.seller_id
GROUP BY 1, 2, 3