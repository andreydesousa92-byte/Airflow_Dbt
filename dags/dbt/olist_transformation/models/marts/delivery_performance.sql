SELECT
    c.state,
    c.city,
    COUNT(o.order_id)                                               AS qty_orders,

    ROUND(AVG(
        EXTRACT(EPOCH FROM (o.approved_at - o.purchased_at)) / 86400
    )::NUMERIC, 1)                                                  AS avg_days_to_approval,

    ROUND(AVG(
        EXTRACT(EPOCH FROM (o.delivered_carrier_at - o.approved_at)) / 86400
    )::NUMERIC, 1)                                                  AS avg_days_approval_to_carrier,

    ROUND(AVG(
        EXTRACT(EPOCH FROM (o.delivered_customer_at - o.delivered_carrier_at)) / 86400
    )::NUMERIC, 1)                                                  AS avg_days_carrier_to_customer,

    ROUND(AVG(
        EXTRACT(EPOCH FROM (o.delivered_customer_at - o.purchased_at)) / 86400
    )::NUMERIC, 1)                                                  AS avg_days_total,

    ROUND(AVG(
        EXTRACT(EPOCH FROM (o.estimated_delivery_at - o.delivered_customer_at)) / 86400
    )::NUMERIC, 1)                                                  AS avg_days_vs_estimate,

    ROUND(100.0 * SUM(
        CASE WHEN o.delivered_customer_at <= o.estimated_delivery_at
        THEN 1 ELSE 0 END
    ) / COUNT(o.order_id), 1)                                       AS pct_on_time

FROM {{ ref('stg_orders') }} o
JOIN {{ ref('stg_customers') }} c ON o.customer_id = c.customer_id
WHERE o.is_delivered = true
GROUP BY 1, 2
