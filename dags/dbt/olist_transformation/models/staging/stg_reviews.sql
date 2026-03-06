SELECT
    review_id,
    order_id,
    review_score::INT                   AS review_score,
    review_comment_title                AS comment_title,
    review_comment_message              AS comment_message,
    review_creation_date::TIMESTAMP     AS created_at,
    review_answer_timestamp::TIMESTAMP  AS answered_at
FROM {{ source('public', 'reviews') }}