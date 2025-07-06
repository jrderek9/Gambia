{{ config(
    materialized='view'
) }}

WITH daily_revenue AS (
    SELECT
        payment_date,
        tax_type,
        payment_channel,
        SUM(amount) AS daily_amount,
        COUNT(DISTINCT taxpayer_id) AS unique_taxpayers,
        COUNT(*) AS transaction_count
    FROM {{ ref('stg_payments') }}
    GROUP BY payment_date, tax_type, payment_channel
),

monthly_revenue AS (
    SELECT
        DATE_TRUNC('month', payment_date) AS revenue_month,
        tax_type,
        SUM(amount) AS monthly_amount,
        COUNT(DISTINCT taxpayer_id) AS unique_taxpayers,
        COUNT(*) AS transaction_count,
        AVG(amount) AS avg_payment_amount
    FROM {{ ref('stg_payments') }}
    GROUP BY DATE_TRUNC('month', payment_date), tax_type
),

regional_revenue AS (
    SELECT
        t.region,
        t.district,
        p.tax_type,
        DATE_TRUNC('month', p.payment_date) AS revenue_month,
        SUM(p.amount) AS amount,
        COUNT(DISTINCT p.taxpayer_id) AS unique_taxpayers
    FROM {{ ref('stg_payments') }} p
    JOIN {{ ref('stg_taxpayers') }} t ON p.taxpayer_id = t.taxpayer_id
    GROUP BY t.region, t.district, p.tax_type, DATE_TRUNC('month', p.payment_date)
),

sector_revenue AS (
    SELECT
        t.business_sector,
        t.business_subsector,
        p.tax_type,
        DATE_TRUNC('quarter', p.payment_date) AS revenue_quarter,
        SUM(p.amount) AS amount,
        COUNT(DISTINCT p.taxpayer_id) AS unique_taxpayers,
        AVG(p.amount) AS avg_payment
    FROM {{ ref('stg_payments') }} p
    JOIN {{ ref('stg_taxpayers') }} t ON p.taxpayer_id = t.taxpayer_id
    WHERE t.business_sector IS NOT NULL
    GROUP BY t.business_sector, t.business_subsector, p.tax_type, DATE_TRUNC('quarter', p.payment_date)
)

SELECT
    'daily' AS aggregation_level,
    payment_date AS period_date,
    NULL AS period_month,
    NULL AS period_quarter,
    tax_type,
    payment_channel,
    NULL AS region,
    NULL AS district,
    NULL AS business_sector,
    daily_amount AS amount,
    unique_taxpayers,
    transaction_count,
    NULL AS avg_payment_amount
FROM daily_revenue

UNION ALL

SELECT
    'monthly' AS aggregation_level,
    revenue_month AS period_date,
    revenue_month AS period_month,
    NULL AS period_quarter,
    tax_type,
    NULL AS payment_channel,
    NULL AS region,
    NULL AS district,
    NULL AS business_sector,
    monthly_amount AS amount,
    unique_taxpayers,
    transaction_count,
    avg_payment_amount
FROM monthly_revenue

UNION ALL

SELECT
    'regional' AS aggregation_level,
    revenue_month AS period_date,
    revenue_month AS period_month,
    NULL AS period_quarter,
    tax_type,
    NULL AS payment_channel,
    region,
    district,
    NULL AS business_sector,
    amount,
    unique_taxpayers,
    NULL AS transaction_count,
    NULL AS avg_payment_amount
FROM regional_revenue

UNION ALL

SELECT
    'sector' AS aggregation_level,
    revenue_quarter AS period_date,
    NULL AS period_month,
    revenue_quarter AS period_quarter,
    tax_type,
    NULL AS payment_channel,
    NULL AS region,
    NULL AS district,
    business_sector,
    amount,
    unique_taxpayers,
    NULL AS transaction_count,
    avg_payment AS avg_payment_amount
FROM sector_revenue