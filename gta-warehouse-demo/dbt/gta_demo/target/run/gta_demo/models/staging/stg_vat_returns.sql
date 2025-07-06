
  create view "gta_warehouse"."analytics_staging"."stg_vat_returns__dbt_tmp"
    
    
  as (
    

WITH source AS (
    SELECT * FROM "gta_warehouse"."raw"."vat_returns"
)

SELECT
    return_id,
    taxpayer_id,
    period_year,
    period_quarter,
    filing_date,
    due_date,
    total_sales,
    taxable_sales,
    exempt_sales,
    export_sales,
    output_vat,
    total_purchases,
    input_vat,
    net_vat_payable,
    status,
    created_at,
    
    -- Calculated fields
    CASE 
        WHEN filing_date IS NULL THEN 'Not Filed'
        WHEN filing_date > due_date THEN 'Late'
        ELSE 'On Time'
    END AS filing_status,
    
    EXTRACT(DAY FROM filing_date - due_date) AS days_late,
    
    -- VAT efficiency ratio
    CASE 
        WHEN total_sales > 0 THEN output_vat / total_sales
        ELSE 0
    END AS vat_rate_effective
    
FROM source
  );