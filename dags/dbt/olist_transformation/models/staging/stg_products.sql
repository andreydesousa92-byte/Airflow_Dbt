SELECT
    product_id,
    product_category_name               AS category_name,
    product_weight_g::NUMERIC           AS weight_g,
    product_length_cm::NUMERIC          AS length_cm,
    product_height_cm::NUMERIC          AS height_cm,
    product_width_cm::NUMERIC           AS width_cm,
    product_photos_qty::INT             AS photos_qty
FROM {{ source('public', 'products') }}