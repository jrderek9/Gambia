{{ config(
    materialized='table',
    indexes=[
      {'columns': ['metric_date', 'metric_type']},
      {'columns': ['region']},
      {'columns': ['tax_type']}
    ]
) }}

WITH current_period_revenue AS (
    SELECT
        DATE_TRUNC('day', payment_date) AS metric_date,
        'daily_revenue' AS metric_type,
        tax_type,
        NULL AS region,
        NULL AS payment_channel,
        SUM(amount) AS revenue_amount,
        COUNT(DISTINCT taxpayer_id) AS unique_taxpayers,
        COUNT(*) AS transaction_count
    FROM {{ ref('stg_payments') }}
    WHERE payment_date >= CURRENT_DATE - INTERVAL '2 years'
    GROUP BY DATE_TRUNC('day', payment_date), tax_type
),

monthly_revenue AS (
    SELECT
        DATE_TRUNC('month', payment_date) AS metric_date,
        'monthly_revenue' AS metric_type,
        tax_type,
        NULL AS region,
        NULL AS payment_channel,
        SUM(amount) AS revenue_amount,
        COUNT(DISTINCT taxpayer_id) AS unique_taxpayers,
        COUNT(*) AS transaction_count
    FROM {{ ref('stg_payments') }}
    GROUP BY DATE_TRUNC('month', payment_date), tax_type
),

regional_revenue AS (
    SELECT
        DATE_TRUNC('month', p.payment_date) AS metric_date,
        'regional_revenue' AS metric_type,
        p.tax_type,
        t.region,
        NULL AS payment_channel,
        SUM(p.amount) AS revenue_amount,
        COUNT(DISTINCT p.taxpayer_id) AS unique_taxpayers,
        COUNT(*) AS transaction_count
    FROM {{ ref('stg_payments') }} p
    JOIN {{ ref('stg_taxpayers') }} t ON p.taxpayer_id = t.taxpayer_id
    GROUP BY DATE_TRUNC('month', p.payment_date), p.tax_type, t.region
),

channel_revenue AS (
    SELECT
        DATE_TRUNC('month', payment_date) AS metric_date,
        'channel_revenue' AS metric_type,
        tax_type,
        NULL AS region,
        payment_channel,
        SUM(amount) AS revenue_amount,
        COUNT(DISTINCT taxpayer_id) AS unique_taxpayers,
        COUNT(*) AS transaction_count
    FROM {{ ref('stg_payments') }}
    GROUP BY DATE_TRUNC('month', payment_date), tax_type, payment_channel
),

yearly_targets AS (
    SELECT
        DATE_TRUNC('year', metric_date) AS target_year,
        tax_type,
        SUM(revenue_amount) * 1.15 AS annual_target -- 15% growth target
    FROM monthly_revenue
    WHERE metric_date >= CURRENT_DATE - INTERVAL '1 year' 
      AND metric_date < CURRENT_DATE
    GROUP BY DATE_TRUNC('year', metric_date), tax_type
),

cumulative_revenue AS (
    SELECT
        metric_date,
        'ytd_revenue' AS metric_type,
        tax_type,
        NULL AS region,
        NULL AS payment_channel,
        SUM(revenue_amount) OVER (
            PARTITION BY DATE_TRUNC('year', metric_date), tax_type 
            ORDER BY metric_date
        ) AS revenue_amount,
        NULL AS unique_taxpayers,
        NULL AS transaction_count
    FROM monthly_revenue
)

-- Combine all metrics
SELECT * FROM current_period_revenue
UNION ALL
SELECT * FROM monthly_revenue
UNION ALL
SELECT * FROM regional_revenue
UNION ALL
SELECT * FROM channel_revenue
UNION ALL
SELECT * FROM cumulative_revenue

-- Add YoY comparison
UNION ALL
SELECT
    m1.metric_date,
    'yoy_growth' AS metric_type,
    m1.tax_type,
    NULL AS region,
    NULL AS payment_channel,
    (m1.revenue_amount - COALESCE(m2.revenue_amount, 0)) / NULLIF(m2.revenue_amount, 0) * 100 AS revenue_amount,
    NULL AS unique_taxpayers,
    NULL AS transaction_count
FROM monthly_revenue m1
LEFT JOIN monthly_revenue m2 
    ON m1.tax_type = m2.tax_type 
    AND m2.metric_date = m1.metric_date - INTERVAL '1 year'

-- Add target vs actual
UNION ALL
SELECT
    m.metric_date,
    'target_vs_actual' AS metric_type,
    m.tax_type,
    NULL AS region,
    NULL AS payment_channel,
    (m.revenue_amount / (t.annual_target / 12)) * 100 AS revenue_amount, -- % of monthly target
    NULL AS unique_taxpayers,
    NULL AS transaction_count
FROM monthly_revenue m
JOIN yearly_targets t 
    ON m.tax_type = t.tax_type 
    AND DATE_TRUNC('year', m.metric_date) = DATE_TRUNC('year', CURRENT_DATE)