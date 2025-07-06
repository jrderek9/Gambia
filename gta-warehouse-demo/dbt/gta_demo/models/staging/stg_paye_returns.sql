{{ config(
    materialized='view'
) }}

WITH source AS (
    SELECT * FROM {{ source('raw', 'paye_returns') }}
)

SELECT
    return_id,
    taxpayer_id,
    period_year,
    period_month,
    filing_date,
    due_date,
    employee_count,
    gross_salaries,
    paye_tax,
    social_security,
    total_deductions,
    net_payment,
    status,
    created_at,
    
    -- Calculated fields
    CASE 
        WHEN filing_date IS NULL THEN 'Not Filed'
        WHEN filing_date > due_date THEN 'Late'
        ELSE 'On Time'
    END AS filing_status,
    
    EXTRACT(DAY FROM filing_date - due_date) AS days_late
    
FROM source