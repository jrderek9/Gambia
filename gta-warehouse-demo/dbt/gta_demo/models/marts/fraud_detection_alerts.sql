{{ config(
    materialized='table',
    indexes=[
      {'columns': ['taxpayer_id']},
      {'columns': ['alert_priority']},
      {'columns': ['created_date']}
    ]
) }}

WITH taxpayer_metrics AS (
    SELECT * FROM {{ ref('taxpayer_360_view') }}
),

-- Detect sudden drops in VAT declarations
vat_drop_detection AS (
    SELECT
        v1.taxpayer_id,
        v1.period_year,
        v1.period_quarter,
        v1.total_sales AS current_sales,
        AVG(v2.total_sales) AS avg_previous_sales,
        (v1.total_sales - AVG(v2.total_sales)) / NULLIF(AVG(v2.total_sales), 0) * 100 AS sales_change_pct
    FROM {{ ref('stg_vat_returns') }} v1
    JOIN {{ ref('stg_vat_returns') }} v2 
        ON v1.taxpayer_id = v2.taxpayer_id
        AND v2.period_year * 4 + v2.period_quarter < v1.period_year * 4 + v1.period_quarter
        AND v2.period_year * 4 + v2.period_quarter >= (v1.period_year * 4 + v1.period_quarter) - 4
    WHERE v1.status = 'Filed'
    GROUP BY v1.taxpayer_id, v1.period_year, v1.period_quarter, v1.total_sales
    HAVING COUNT(v2.return_id) >= 2
),

-- Detect PAYE vs revenue inconsistencies
paye_revenue_mismatch AS (
    SELECT
        p.taxpayer_id,
        p.period_year,
        SUM(p.gross_salaries) AS annual_salaries,
        t.annual_turnover,
        (SUM(p.gross_salaries) / NULLIF(t.annual_turnover, 0)) * 100 AS salary_to_revenue_ratio
    FROM {{ ref('stg_paye_returns') }} p
    JOIN {{ ref('stg_taxpayers') }} t ON p.taxpayer_id = t.taxpayer_id
    WHERE p.status = 'Filed'
      AND t.annual_turnover > 0
      AND t.taxpayer_type = 'Corporate'
    GROUP BY p.taxpayer_id, p.period_year, t.annual_turnover
),

-- Detect payment anomalies
payment_anomalies AS (
    SELECT
        taxpayer_id,
        COUNT(DISTINCT payment_channel) AS channel_switches,
        STDDEV(amount) / NULLIF(AVG(amount), 0) AS payment_variance,
        COUNT(CASE WHEN EXTRACT(hour FROM created_at::time) NOT BETWEEN 8 AND 18 THEN 1 END) AS odd_hour_payments
    FROM {{ ref('stg_payments') }}
    WHERE payment_date >= CURRENT_DATE - INTERVAL '6 months'
    GROUP BY taxpayer_id
),

-- Peer comparison anomalies
peer_anomalies AS (
    SELECT
        t1.taxpayer_id,
        t1.business_sector,
        t1.business_size_category,
        t1.total_tax_paid,
        AVG(t2.total_tax_paid) AS peer_avg_tax,
        (t1.total_tax_paid - AVG(t2.total_tax_paid)) / NULLIF(AVG(t2.total_tax_paid), 0) * 100 AS variance_from_peers
    FROM taxpayer_metrics t1
    JOIN taxpayer_metrics t2 
        ON t1.business_sector = t2.business_sector
        AND t1.business_size_category = t2.business_size_category
        AND t1.taxpayer_id != t2.taxpayer_id
    WHERE t1.total_tax_paid > 0
    GROUP BY t1.taxpayer_id, t1.business_sector, t1.business_size_category, t1.total_tax_paid
    HAVING COUNT(t2.taxpayer_id) >= 5
)

-- Generate fraud alerts
SELECT
    ROW_NUMBER() OVER (ORDER BY fraud_score DESC) AS alert_id,
    taxpayer_id,
    taxpayer_name,
    alert_type,
    alert_description,
    fraud_score,
    CASE
        WHEN fraud_score >= 0.8 THEN 'Critical'
        WHEN fraud_score >= 0.6 THEN 'High'
        WHEN fraud_score >= 0.4 THEN 'Medium'
        ELSE 'Low'
    END AS alert_priority,
    potential_revenue_impact,
    recommended_action,
    CURRENT_DATE AS created_date,
    'Open' AS status
FROM (
    -- VAT drop alerts
    SELECT
        vd.taxpayer_id,
        t.name AS taxpayer_name,
        'Sudden VAT Declaration Drop' AS alert_type,
        CONCAT('VAT sales dropped by ', ROUND(ABS(vd.sales_change_pct), 1), '% in Q', vd.period_quarter, ' ', vd.period_year) AS alert_description,
        LEAST(ABS(vd.sales_change_pct) / 100, 1) AS fraud_score,
        (vd.avg_previous_sales - vd.current_sales) * 0.15 AS potential_revenue_impact,
        'Review recent VAT returns and request supporting documentation' AS recommended_action
    FROM vat_drop_detection vd
    JOIN taxpayer_metrics t ON vd.taxpayer_id = t.taxpayer_id
    WHERE vd.sales_change_pct < -30
    
    UNION ALL
    
    -- PAYE mismatch alerts
    SELECT
        pr.taxpayer_id,
        t.name AS taxpayer_name,
        'PAYE vs Revenue Inconsistency' AS alert_type,
        CONCAT('Salary to revenue ratio is ', ROUND(pr.salary_to_revenue_ratio, 1), '% - industry average is 25-40%') AS alert_description,
        CASE
            WHEN pr.salary_to_revenue_ratio < 10 THEN 0.8
            WHEN pr.salary_to_revenue_ratio < 15 THEN 0.6
            ELSE 0.4
        END AS fraud_score,
        pr.annual_salaries * 0.1 AS potential_revenue_impact,
        'Audit PAYE returns against actual employee records' AS recommended_action
    FROM paye_revenue_mismatch pr
    JOIN taxpayer_metrics t ON pr.taxpayer_id = t.taxpayer_id
    WHERE pr.salary_to_revenue_ratio < 20 OR pr.salary_to_revenue_ratio > 80
    
    UNION ALL
    
    -- Payment anomaly alerts
    SELECT
        pa.taxpayer_id,
        t.name AS taxpayer_name,
        'Suspicious Payment Patterns' AS alert_type,
        CONCAT('High payment variance (', ROUND(pa.payment_variance * 100, 1), '%) with ', pa.channel_switches, ' channel switches') AS alert_description,
        LEAST((pa.payment_variance + pa.channel_switches * 0.1), 1) AS fraud_score,
        t.total_tax_paid * 0.05 AS potential_revenue_impact,
        'Investigate payment methods and transaction patterns' AS recommended_action
    FROM payment_anomalies pa
    JOIN taxpayer_metrics t ON pa.taxpayer_id = t.taxpayer_id
    WHERE pa.payment_variance > 2 OR pa.channel_switches > 3
    
    UNION ALL
    
    -- Peer comparison alerts
    SELECT
        pa.taxpayer_id,
        t.name AS taxpayer_name,
        'Below Peer Tax Contribution' AS alert_type,
        CONCAT('Paying ', ROUND(ABS(pa.variance_from_peers), 1), '% less than similar businesses in ', pa.business_sector) AS alert_description,
        LEAST(ABS(pa.variance_from_peers) / 100, 1) AS fraud_score,
        (pa.peer_avg_tax - pa.total_tax_paid) AS potential_revenue_impact,
        'Benchmark against similar businesses and conduct detailed audit' AS recommended_action
    FROM peer_anomalies pa
    JOIN taxpayer_metrics t ON pa.taxpayer_id = t.taxpayer_id
    WHERE pa.variance_from_peers < -50
    
    UNION ALL
    
    -- Chronic non-compliance alerts
    SELECT
        taxpayer_id,
        name AS taxpayer_name,
        'Chronic Non-Compliance' AS alert_type,
        CONCAT('Filing compliance rate only ', ROUND(filing_compliance_rate * 100, 1), '% with ', fraud_alert_count, ' previous alerts') AS alert_description,
        GREATEST(1 - filing_compliance_rate, fraud_risk_score) AS fraud_score,
        total_tax_paid * 0.2 AS potential_revenue_impact,
        'Escalate to enforcement team for immediate action' AS recommended_action
    FROM taxpayer_metrics
    WHERE filing_compliance_rate < 0.5 OR fraud_alert_count > 2
) fraud_alerts
WHERE fraud_score >= 0.4