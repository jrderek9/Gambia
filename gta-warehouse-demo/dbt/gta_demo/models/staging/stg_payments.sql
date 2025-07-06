{{ config(
    materialized='view'
) }}

WITH source AS (
    SELECT * FROM {{ source('raw', 'payments') }}
)

SELECT
    payment_id,
    taxpayer_id,
    payment_date,
    payment_channel,
    payment_provider,
    tax_type,
    period_year,
    period_month,
    amount,
    reference_number,
    status,
    created_at,
    
    -- Extract payment delay information
    EXTRACT(YEAR FROM payment_date) AS payment_year,
    EXTRACT(MONTH FROM payment_date) AS payment_month,
    EXTRACT(QUARTER FROM payment_date) AS payment_quarter
    
FROM source
WHERE status = 'Completed'