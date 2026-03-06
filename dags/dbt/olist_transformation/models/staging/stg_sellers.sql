SELECT
    seller_id,
    seller_zip_code_prefix              AS zip_code,
    UPPER(seller_city)                  AS city,
    seller_state                        AS state
FROM {{ source('public', 'sellers') }}