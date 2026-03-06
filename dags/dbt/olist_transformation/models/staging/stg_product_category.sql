SELECT
    product_category_name               AS category_name,
    product_category_name_english       AS category_name_english
FROM {{ source('public', 'products_category') }}