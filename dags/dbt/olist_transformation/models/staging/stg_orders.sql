SELECT
    order_id,
    customer_id,
    order_status,
    order_status = 'delivered'                          AS is_delivered,
    order_purchase_timestamp::TIMESTAMP                 AS purchased_at,
    order_approved_at::TIMESTAMP                        AS approved_at,
    order_delivered_carrier_date::TIMESTAMP             AS delivered_carrier_at,
    order_delivered_customer_date::TIMESTAMP            AS delivered_customer_at,
    order_estimated_delivery_date::TIMESTAMP            AS estimated_delivery_at
FROM {{ source('public', 'orders') }}