

WITH source AS (
    SELECT * FROM "gta_warehouse"."raw"."taxpayers"
)

SELECT
    taxpayer_id,
    tin,
    name,
    taxpayer_type,
    registration_date,
    email,
    phone,
    address_line1,
    address_line2,
    district,
    region,
    business_sector,
    business_subsector,
    employee_count,
    annual_turnover,
    risk_category,
    compliance_score,
    created_at,
    updated_at
FROM source