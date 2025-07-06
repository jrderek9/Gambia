-- Metabase Setup Queries for GTA Dashboards
-- These queries can be used to create cards and dashboards in Metabase

-- 1. EXECUTIVE REVENUE DASHBOARD

-- Total Revenue by Tax Type (Latest Available Month)
SELECT 
    tax_type,
    SUM(revenue_amount) as total_revenue,
    COUNT(DISTINCT unique_taxpayers) as taxpayers,
    AVG(revenue_amount / NULLIF(transaction_count, 0)) as avg_transaction
FROM analytics.revenue_dashboard_metrics
WHERE metric_type = 'monthly_revenue'
  AND metric_date = (SELECT MAX(metric_date) FROM analytics.revenue_dashboard_metrics WHERE metric_type = 'monthly_revenue')
GROUP BY tax_type
ORDER BY total_revenue DESC;

-- Revenue Trend (Last 12 Months)
SELECT 
    metric_date,
    tax_type,
    revenue_amount
FROM analytics.revenue_dashboard_metrics
WHERE metric_type = 'monthly_revenue'
  AND metric_date >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '12 months')
ORDER BY metric_date, tax_type;

-- Regional Revenue Heat Map
SELECT 
    region,
    SUM(revenue_amount) as total_revenue,
    COUNT(DISTINCT unique_taxpayers) as taxpayers
FROM analytics.revenue_dashboard_metrics
WHERE metric_type = 'regional_revenue'
  AND metric_date = DATE_TRUNC('month', CURRENT_DATE)
GROUP BY region
ORDER BY total_revenue DESC;

-- Target vs Actual Performance
SELECT 
    tax_type,
    revenue_amount as performance_percentage
FROM analytics.revenue_dashboard_metrics
WHERE metric_type = 'target_vs_actual'
  AND metric_date = DATE_TRUNC('month', CURRENT_DATE);

-- YoY Growth Comparison
SELECT 
    tax_type,
    metric_date,
    revenue_amount as yoy_growth_percentage
FROM analytics.revenue_dashboard_metrics
WHERE metric_type = 'yoy_growth'
  AND metric_date >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '12 months')
ORDER BY metric_date DESC;

-- Payment Channel Distribution
SELECT 
    payment_channel,
    SUM(revenue_amount) as total_revenue,
    COUNT(DISTINCT unique_taxpayers) as taxpayers,
    (SUM(revenue_amount) / SUM(SUM(revenue_amount)) OVER ()) * 100 as percentage
FROM analytics.revenue_dashboard_metrics
WHERE metric_type = 'channel_revenue'
  AND metric_date = DATE_TRUNC('month', CURRENT_DATE)
GROUP BY payment_channel
ORDER BY total_revenue DESC;

-- 2. FRAUD DETECTION DASHBOARD

-- Active Fraud Alerts by Priority
SELECT 
    alert_priority,
    COUNT(*) as alert_count,
    AVG(fraud_score) as avg_risk_score,
    SUM(potential_revenue_impact) as total_impact
FROM analytics.fraud_detection_alerts
WHERE status = 'Open'
GROUP BY alert_priority
ORDER BY 
    CASE alert_priority 
        WHEN 'Critical' THEN 1 
        WHEN 'High' THEN 2 
        WHEN 'Medium' THEN 3 
        ELSE 4 
    END;

-- Top 20 High-Risk Taxpayers
SELECT 
    taxpayer_name,
    alert_type,
    fraud_score,
    potential_revenue_impact,
    recommended_action
FROM analytics.fraud_detection_alerts
WHERE status = 'Open'
ORDER BY fraud_score DESC
LIMIT 20;

-- Fraud Alerts by Type
SELECT 
    alert_type,
    COUNT(*) as count,
    AVG(fraud_score) as avg_score,
    SUM(potential_revenue_impact) as total_impact
FROM analytics.fraud_detection_alerts
WHERE status = 'Open'
GROUP BY alert_type
ORDER BY count DESC;

-- Risk Distribution by Sector
SELECT 
    t.business_sector,
    COUNT(DISTINCT t.taxpayer_id) as total_taxpayers,
    COUNT(DISTINCT f.taxpayer_id) as flagged_taxpayers,
    (COUNT(DISTINCT f.taxpayer_id)::NUMERIC / COUNT(DISTINCT t.taxpayer_id)) * 100 as risk_percentage
FROM analytics.taxpayer_360_view t
LEFT JOIN analytics.fraud_detection_alerts f 
    ON t.taxpayer_id = f.taxpayer_id 
    AND f.status = 'Open'
WHERE t.business_sector IS NOT NULL
GROUP BY t.business_sector
ORDER BY risk_percentage DESC;

-- 3. TAXPAYER 360 VIEW

-- Individual Taxpayer Profile (parameterized)
SELECT 
    name,
    tin,
    taxpayer_type,
    region || ', ' || district as location,
    business_sector,
    business_subsector,
    years_registered,
    employee_count,
    annual_turnover,
    filing_compliance_rate * 100 as compliance_percentage,
    total_tax_paid,
    risk_category_calculated,
    engagement_score * 100 as engagement_percentage
FROM analytics.taxpayer_360_view
WHERE taxpayer_id = {{taxpayer_id}};

-- Taxpayer Payment History
SELECT 
    payment_date,
    tax_type,
    amount,
    payment_channel,
    payment_provider
FROM raw.payments
WHERE taxpayer_id = {{taxpayer_id}}
  AND status = 'Completed'
ORDER BY payment_date DESC
LIMIT 50;

-- Taxpayer Compliance Timeline
SELECT 
    COALESCE(pr.period_year, vr.period_year) as year,
    COALESCE(pr.period_month, vr.period_quarter * 3) as period,
    CASE 
        WHEN pr.return_id IS NOT NULL THEN 'PAYE'
        WHEN vr.return_id IS NOT NULL THEN 'VAT'
    END as tax_type,
    COALESCE(pr.status, vr.status) as status,
    COALESCE(pr.filing_date, vr.filing_date) as filing_date,
    COALESCE(pr.due_date, vr.due_date) as due_date,
    CASE 
        WHEN COALESCE(pr.filing_date, vr.filing_date) > COALESCE(pr.due_date, vr.due_date) 
        THEN 'Late' 
        ELSE 'On Time' 
    END as filing_status
FROM raw.paye_returns pr
FULL OUTER JOIN raw.vat_returns vr 
    ON pr.taxpayer_id = vr.taxpayer_id 
    AND pr.period_year = vr.period_year
WHERE COALESCE(pr.taxpayer_id, vr.taxpayer_id) = {{taxpayer_id}}
ORDER BY year DESC, period DESC;

-- Peer Comparison
WITH taxpayer_info AS (
    SELECT 
        business_sector,
        business_size_category,
        total_tax_paid
    FROM analytics.taxpayer_360_view
    WHERE taxpayer_id = {{taxpayer_id}}
)
SELECT 
    t.name,
    t.total_tax_paid,
    t.filing_compliance_rate * 100 as compliance_rate,
    t.employee_count,
    t.annual_turnover
FROM analytics.taxpayer_360_view t
JOIN taxpayer_info ti 
    ON t.business_sector = ti.business_sector 
    AND t.business_size_category = ti.business_size_category
WHERE t.taxpayer_id != {{taxpayer_id}}
ORDER BY ABS(t.total_tax_paid - ti.total_tax_paid)
LIMIT 10;

-- 4. SELF-SERVICE ANALYTICS

-- Revenue by Business Sector
SELECT 
    business_sector,
    tax_type,
    SUM(amount) as total_revenue,
    COUNT(DISTINCT p.taxpayer_id) as taxpayers,
    AVG(amount) as avg_payment
FROM raw.payments p
JOIN raw.taxpayers t ON p.taxpayer_id = t.taxpayer_id
WHERE p.status = 'Completed'
  AND p.payment_date >= {{start_date}}
  AND p.payment_date <= {{end_date}}
  AND t.business_sector IS NOT NULL
GROUP BY business_sector, tax_type
ORDER BY total_revenue DESC;

-- Compliance Rate by Region
SELECT 
    region,
    AVG(filing_compliance_rate) * 100 as avg_compliance_rate,
    COUNT(*) as taxpayer_count,
    SUM(CASE WHEN chronic_late_filer THEN 1 ELSE 0 END) as chronic_late_filers
FROM analytics.taxpayer_360_view
GROUP BY region
ORDER BY avg_compliance_rate DESC;

-- Payment Delays Analysis
SELECT 
    DATE_TRUNC('month', p.payment_date) as month,
    tax_type,
    AVG(EXTRACT(DAYS FROM p.payment_date - COALESCE(pr.due_date, vr.due_date))) as avg_delay_days,
    COUNT(*) as payment_count
FROM raw.payments p
LEFT JOIN raw.paye_returns pr ON p.reference_number = pr.return_id
LEFT JOIN raw.vat_returns vr ON p.reference_number = vr.return_id
WHERE p.status = 'Completed'
  AND (pr.due_date IS NOT NULL OR vr.due_date IS NOT NULL)
  AND p.payment_date >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '12 months')
GROUP BY DATE_TRUNC('month', p.payment_date), tax_type
ORDER BY month DESC, tax_type;

-- 5. REVENUE FORECAST DASHBOARD

-- Next 30 Days Revenue Forecast
SELECT 
    forecast_date,
    tax_type,
    predicted_revenue,
    lower_bound,
    upper_bound
FROM analytics.revenue_forecasts
WHERE forecast_date BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL '30 days'
ORDER BY forecast_date, tax_type;

-- Forecast vs Actual Comparison
SELECT 
    f.forecast_date,
    f.tax_type,
    f.predicted_revenue,
    COALESCE(SUM(p.amount), 0) as actual_revenue,
    (COALESCE(SUM(p.amount), 0) - f.predicted_revenue) as variance,
    CASE 
        WHEN f.predicted_revenue > 0 
        THEN ((COALESCE(SUM(p.amount), 0) - f.predicted_revenue) / f.predicted_revenue) * 100 
        ELSE 0 
    END as variance_percentage
FROM analytics.revenue_forecasts f
LEFT JOIN raw.payments p 
    ON f.forecast_date = p.payment_date 
    AND f.tax_type = p.tax_type 
    AND p.status = 'Completed'
WHERE f.forecast_date < CURRENT_DATE
  AND f.forecast_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY f.forecast_date, f.tax_type, f.predicted_revenue
ORDER BY f.forecast_date DESC, f.tax_type;