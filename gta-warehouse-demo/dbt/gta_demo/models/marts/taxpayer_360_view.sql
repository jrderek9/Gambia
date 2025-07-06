{{ config(
    materialized='table',
    indexes=[
      {'columns': ['taxpayer_id'], 'unique': True},
      {'columns': ['risk_score']},
      {'columns': ['region', 'district']}
    ]
) }}

WITH taxpayer_base AS (
    SELECT * FROM {{ ref('stg_taxpayers') }}
),

compliance_metrics AS (
    SELECT * FROM {{ ref('int_taxpayer_compliance') }}
),

revenue_contribution AS (
    SELECT
        taxpayer_id,
        SUM(CASE WHEN tax_type = 'PAYE' THEN amount ELSE 0 END) AS total_paye_paid,
        SUM(CASE WHEN tax_type = 'VAT' THEN amount ELSE 0 END) AS total_vat_paid,
        SUM(CASE WHEN tax_type = 'Corporate Tax' THEN amount ELSE 0 END) AS total_corporate_tax_paid,
        SUM(amount) AS total_tax_paid,
        MIN(payment_date) AS first_payment_date,
        MAX(payment_date) AS last_payment_date,
        COUNT(DISTINCT DATE_TRUNC('month', payment_date)) AS active_payment_months
    FROM {{ ref('stg_payments') }}
    GROUP BY taxpayer_id
),

external_assets AS (
    SELECT
        t.taxpayer_id,
        COUNT(DISTINCT c.company_reg_no) AS companies_owned,
        COUNT(DISTINCT v.vehicle_reg_no) AS vehicles_owned,
        COUNT(DISTINCT l.property_id) AS properties_owned,
        SUM(v.purchase_value) AS total_vehicle_value,
        SUM(l.valuation) AS total_property_value
    FROM taxpayer_base t
    LEFT JOIN raw.companies_registry c ON t.taxpayer_id = c.taxpayer_id
    LEFT JOIN raw.vehicle_registry v ON t.taxpayer_id = v.taxpayer_id
    LEFT JOIN raw.land_registry l ON t.taxpayer_id = l.taxpayer_id
    GROUP BY t.taxpayer_id
),

fraud_indicators AS (
    SELECT
        taxpayer_id,
        COUNT(*) AS fraud_alert_count,
        MAX(risk_score) AS max_fraud_risk_score,
        STRING_AGG(alert_type, '; ') AS fraud_alert_types
    FROM analytics.fraud_alerts
    WHERE status IN ('Open', 'Under Investigation')
    GROUP BY taxpayer_id
)

SELECT
    -- Basic Information
    t.taxpayer_id,
    t.tin,
    t.name,
    t.taxpayer_type,
    t.registration_date,
    DATE_PART('year', AGE(CURRENT_DATE, t.registration_date)) AS years_registered,
    t.email,
    t.phone,
    t.district,
    t.region,
    t.business_sector,
    t.business_subsector,
    t.employee_count,
    t.annual_turnover,
    
    -- Compliance Metrics
    cm.filing_compliance_rate,
    cm.total_paye_returns_expected,
    cm.paye_returns_filed,
    cm.paye_on_time,
    cm.avg_paye_days_late,
    cm.total_vat_returns_expected,
    cm.vat_returns_filed,
    cm.vat_on_time,
    cm.avg_vat_days_late,
    cm.chronic_late_filer,
    cm.low_filing_rate,
    
    -- Revenue Contribution
    COALESCE(rc.total_paye_paid, 0) AS total_paye_paid,
    COALESCE(rc.total_vat_paid, 0) AS total_vat_paid,
    COALESCE(rc.total_corporate_tax_paid, 0) AS total_corporate_tax_paid,
    COALESCE(rc.total_tax_paid, 0) AS total_tax_paid,
    rc.first_payment_date,
    rc.last_payment_date,
    COALESCE(rc.active_payment_months, 0) AS active_payment_months,
    
    -- External Assets
    COALESCE(ea.companies_owned, 0) AS companies_owned,
    COALESCE(ea.vehicles_owned, 0) AS vehicles_owned,
    COALESCE(ea.properties_owned, 0) AS properties_owned,
    COALESCE(ea.total_vehicle_value, 0) AS total_vehicle_value,
    COALESCE(ea.total_property_value, 0) AS total_property_value,
    
    -- Risk Assessment
    COALESCE(fi.fraud_alert_count, 0) AS fraud_alert_count,
    COALESCE(fi.max_fraud_risk_score, 0) AS fraud_risk_score,
    fi.fraud_alert_types,
    
    -- Calculate composite risk score
    CASE
        WHEN COALESCE(fi.max_fraud_risk_score, 0) > 0.7 THEN 'Critical'
        WHEN cm.chronic_late_filer OR cm.low_filing_rate THEN 'High'
        WHEN cm.filing_compliance_rate < 0.5 THEN 'Medium'
        WHEN cm.filing_compliance_rate < 0.8 THEN 'Low'
        ELSE 'Very Low'
    END AS risk_category_calculated,
    
    -- Peer comparison preparation
    CASE 
        WHEN t.annual_turnover < 1000000 THEN 'Small'
        WHEN t.annual_turnover < 10000000 THEN 'Medium'
        WHEN t.annual_turnover < 100000000 THEN 'Large'
        ELSE 'Very Large'
    END AS business_size_category,
    
    -- Engagement score
    (
        CASE WHEN t.email IS NOT NULL THEN 0.2 ELSE 0 END +
        CASE WHEN rc.active_payment_months > 12 THEN 0.3 ELSE rc.active_payment_months * 0.025 END +
        cm.filing_compliance_rate * 0.5
    ) AS engagement_score,
    
    -- Metadata
    CURRENT_TIMESTAMP AS last_updated

FROM taxpayer_base t
LEFT JOIN compliance_metrics cm ON t.taxpayer_id = cm.taxpayer_id
LEFT JOIN revenue_contribution rc ON t.taxpayer_id = rc.taxpayer_id
LEFT JOIN external_assets ea ON t.taxpayer_id = ea.taxpayer_id
LEFT JOIN fraud_indicators fi ON t.taxpayer_id = fi.taxpayer_id