-- Create simplified analytics views for Metabase dashboards

-- Revenue Dashboard Metrics
CREATE OR REPLACE VIEW analytics.revenue_dashboard_metrics AS
WITH monthly_revenue AS (
    SELECT 
        DATE_TRUNC('month', payment_date) AS metric_date,
        'monthly_revenue' AS metric_type,
        tax_type,
        NULL AS region,
        NULL AS payment_channel,
        SUM(amount) AS revenue_amount,
        COUNT(DISTINCT taxpayer_id) AS unique_taxpayers,
        COUNT(*) AS transaction_count
    FROM raw.payments
    WHERE status = 'Completed'
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
    FROM raw.payments p
    JOIN raw.taxpayers t ON p.taxpayer_id = t.taxpayer_id
    WHERE p.status = 'Completed'
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
    FROM raw.payments
    WHERE status = 'Completed'
    GROUP BY DATE_TRUNC('month', payment_date), tax_type, payment_channel
)
SELECT * FROM monthly_revenue
UNION ALL
SELECT * FROM regional_revenue
UNION ALL
SELECT * FROM channel_revenue;

-- Taxpayer 360 View
CREATE OR REPLACE VIEW analytics.taxpayer_360_view AS
WITH payment_summary AS (
    SELECT 
        taxpayer_id,
        SUM(CASE WHEN tax_type = 'PAYE' THEN amount ELSE 0 END) AS total_paye_paid,
        SUM(CASE WHEN tax_type = 'VAT' THEN amount ELSE 0 END) AS total_vat_paid,
        SUM(amount) AS total_tax_paid,
        COUNT(DISTINCT payment_channel) AS payment_channels_used,
        MIN(payment_date) AS first_payment_date,
        MAX(payment_date) AS last_payment_date
    FROM raw.payments
    WHERE status = 'Completed'
    GROUP BY taxpayer_id
),
filing_summary AS (
    SELECT 
        taxpayer_id,
        COUNT(*) AS total_paye_returns,
        COUNT(CASE WHEN status = 'Filed' THEN 1 END) AS paye_returns_filed,
        COUNT(CASE WHEN filing_date <= due_date THEN 1 END) AS paye_on_time
    FROM raw.paye_returns
    GROUP BY taxpayer_id
),
vat_summary AS (
    SELECT 
        taxpayer_id,
        COUNT(*) AS total_vat_returns,
        COUNT(CASE WHEN status = 'Filed' THEN 1 END) AS vat_returns_filed,
        COUNT(CASE WHEN filing_date <= due_date THEN 1 END) AS vat_on_time
    FROM raw.vat_returns
    GROUP BY taxpayer_id
),
fraud_summary AS (
    SELECT 
        taxpayer_id,
        COUNT(*) AS fraud_alert_count,
        MAX(risk_score) AS fraud_risk_score,
        STRING_AGG(alert_type, '; ') AS fraud_alert_types
    FROM analytics.fraud_alerts
    WHERE status = 'Open'
    GROUP BY taxpayer_id
)
SELECT 
    t.*,
    COALESCE(ps.total_paye_paid, 0) AS total_paye_paid,
    COALESCE(ps.total_vat_paid, 0) AS total_vat_paid,
    COALESCE(ps.total_tax_paid, 0) AS total_tax_paid,
    COALESCE(ps.payment_channels_used, 0) AS payment_channels_used,
    ps.first_payment_date,
    ps.last_payment_date,
    COALESCE(fs.total_paye_returns, 0) AS total_paye_returns,
    COALESCE(fs.paye_returns_filed, 0) AS paye_returns_filed,
    COALESCE(fs.paye_on_time, 0) AS paye_on_time,
    COALESCE(vs.total_vat_returns, 0) AS total_vat_returns,
    COALESCE(vs.vat_returns_filed, 0) AS vat_returns_filed,
    COALESCE(vs.vat_on_time, 0) AS vat_on_time,
    CASE 
        WHEN (COALESCE(fs.total_paye_returns, 0) + COALESCE(vs.total_vat_returns, 0)) > 0
        THEN (COALESCE(fs.paye_returns_filed, 0) + COALESCE(vs.vat_returns_filed, 0))::NUMERIC / 
             (COALESCE(fs.total_paye_returns, 0) + COALESCE(vs.total_vat_returns, 0))
        ELSE 0
    END AS filing_compliance_rate,
    COALESCE(fr.fraud_alert_count, 0) AS fraud_alert_count,
    COALESCE(fr.fraud_risk_score, 0) AS fraud_risk_score,
    fr.fraud_alert_types,
    EXTRACT(YEAR FROM AGE(CURRENT_DATE, t.registration_date)) AS years_registered
FROM raw.taxpayers t
LEFT JOIN payment_summary ps ON t.taxpayer_id = ps.taxpayer_id
LEFT JOIN filing_summary fs ON t.taxpayer_id = fs.taxpayer_id
LEFT JOIN vat_summary vs ON t.taxpayer_id = vs.taxpayer_id
LEFT JOIN fraud_summary fr ON t.taxpayer_id = fr.taxpayer_id;

-- Fraud Detection Alerts View
CREATE OR REPLACE VIEW analytics.fraud_detection_alerts AS
SELECT 
    fa.*,
    t.name AS taxpayer_name,
    t.business_sector,
    t.region,
    CASE 
        WHEN fa.risk_score >= 0.8 THEN 'Critical'
        WHEN fa.risk_score >= 0.6 THEN 'High'
        WHEN fa.risk_score >= 0.4 THEN 'Medium'
        ELSE 'Low'
    END AS alert_priority
FROM analytics.fraud_alerts fa
JOIN raw.taxpayers t ON fa.taxpayer_id = t.taxpayer_id
WHERE fa.status = 'Open';