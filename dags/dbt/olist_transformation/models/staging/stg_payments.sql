SELECT
    order_id,
    payment_sequential,
    payment_type,
    payment_installments::INT           AS payment_installments,
    payment_value::NUMERIC              AS payment_value
FROM {{ source('public', 'payments') }}