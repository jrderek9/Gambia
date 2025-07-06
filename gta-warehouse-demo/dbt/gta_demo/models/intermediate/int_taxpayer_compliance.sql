{{ config(
    materialized='view'
) }}

WITH paye_compliance AS (
    SELECT
        taxpayer_id,
        COUNT(*) AS total_paye_returns_expected,
        COUNT(CASE WHEN status = 'Filed' THEN 1 END) AS paye_returns_filed,
        COUNT(CASE WHEN filing_status = 'On Time' THEN 1 END) AS paye_on_time,
        AVG(CASE WHEN days_late > 0 THEN days_late END) AS avg_paye_days_late
    FROM {{ ref('stg_paye_returns') }}
    GROUP BY taxpayer_id
),

vat_compliance AS (
    SELECT
        taxpayer_id,
        COUNT(*) AS total_vat_returns_expected,
        COUNT(CASE WHEN status = 'Filed' THEN 1 END) AS vat_returns_filed,
        COUNT(CASE WHEN filing_status = 'On Time' THEN 1 END) AS vat_on_time,
        AVG(CASE WHEN days_late > 0 THEN days_late END) AS avg_vat_days_late
    FROM {{ ref('stg_vat_returns') }}
    GROUP BY taxpayer_id
),

payment_behavior AS (
    SELECT
        p.taxpayer_id,
        COUNT(DISTINCT p.payment_id) AS total_payments,
        SUM(p.amount) AS total_amount_paid,
        COUNT(DISTINCT p.payment_channel) AS payment_channels_used,
        
        -- Payment timing analysis
        COUNT(CASE 
            WHEN p.payment_date <= COALESCE(paye.due_date, vat.due_date) 
            THEN 1 
        END) AS on_time_payments
        
    FROM {{ ref('stg_payments') }} p
    LEFT JOIN {{ ref('stg_paye_returns') }} paye 
        ON p.reference_number = paye.return_id
    LEFT JOIN {{ ref('stg_vat_returns') }} vat 
        ON p.reference_number = vat.return_id
    GROUP BY p.taxpayer_id
)

SELECT
    t.taxpayer_id,
    t.name,
    t.taxpayer_type,
    t.risk_category,
    t.compliance_score AS original_compliance_score,
    
    -- PAYE compliance metrics
    COALESCE(pc.total_paye_returns_expected, 0) AS total_paye_returns_expected,
    COALESCE(pc.paye_returns_filed, 0) AS paye_returns_filed,
    COALESCE(pc.paye_on_time, 0) AS paye_on_time,
    COALESCE(pc.avg_paye_days_late, 0) AS avg_paye_days_late,
    
    -- VAT compliance metrics
    COALESCE(vc.total_vat_returns_expected, 0) AS total_vat_returns_expected,
    COALESCE(vc.vat_returns_filed, 0) AS vat_returns_filed,
    COALESCE(vc.vat_on_time, 0) AS vat_on_time,
    COALESCE(vc.avg_vat_days_late, 0) AS avg_vat_days_late,
    
    -- Payment behavior
    COALESCE(pb.total_payments, 0) AS total_payments,
    COALESCE(pb.total_amount_paid, 0) AS total_amount_paid,
    COALESCE(pb.payment_channels_used, 0) AS payment_channels_used,
    COALESCE(pb.on_time_payments, 0) AS on_time_payments,
    
    -- Calculate overall compliance score
    CASE
        WHEN (COALESCE(pc.total_paye_returns_expected, 0) + COALESCE(vc.total_vat_returns_expected, 0)) > 0
        THEN (
            (COALESCE(pc.paye_returns_filed, 0) + COALESCE(vc.vat_returns_filed, 0))::NUMERIC /
            (COALESCE(pc.total_paye_returns_expected, 0) + COALESCE(vc.total_vat_returns_expected, 0))
        )
        ELSE 0
    END AS filing_compliance_rate,
    
    -- Flag potential issues
    CASE
        WHEN COALESCE(pc.avg_paye_days_late, 0) > 30 OR COALESCE(vc.avg_vat_days_late, 0) > 30 THEN TRUE
        ELSE FALSE
    END AS chronic_late_filer,
    
    CASE
        WHEN (COALESCE(pc.paye_returns_filed, 0) + COALESCE(vc.vat_returns_filed, 0)) < 
             (COALESCE(pc.total_paye_returns_expected, 0) + COALESCE(vc.total_vat_returns_expected, 0)) * 0.5
        THEN TRUE
        ELSE FALSE
    END AS low_filing_rate

FROM {{ ref('stg_taxpayers') }} t
LEFT JOIN paye_compliance pc ON t.taxpayer_id = pc.taxpayer_id
LEFT JOIN vat_compliance vc ON t.taxpayer_id = vc.taxpayer_id
LEFT JOIN payment_behavior pb ON t.taxpayer_id = pb.taxpayer_id