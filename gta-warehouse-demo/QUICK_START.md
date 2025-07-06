# GTA Data Warehouse - Quick Start Guide

## Current Status

Due to Airflow compatibility issues, we're using a simplified setup that provides all the core functionality:

- ✅ **PostgreSQL Database** - Running on port 5432
- ✅ **Metabase BI Tool** - Running on port 3000  
- ✅ **dbt Transformations** - Ready to use
- ⚠️ **Airflow** - Temporarily disabled (use manual pipeline runner instead)

## Services Running

1. **PostgreSQL Database**
   - URL: `localhost:5432`
   - Database: `gta_warehouse`
   - Username: `gta_admin`
   - Password: `gta_secure_pass`

2. **Metabase**
   - URL: http://localhost:3000
   - Already configured with PostgreSQL connection

3. **dbt Container**
   - For running data transformations

## Running the Data Pipeline

Since Airflow has compatibility issues, use the manual pipeline runner:

```bash
# 1. Generate synthetic data
docker-compose -f docker-compose-simple.yml exec dbt python /usr/app/scripts/generate_synthetic_data.py

# 2. Run dbt transformations
docker-compose -f docker-compose-simple.yml exec dbt dbt run --project-dir /usr/app/dbt/gta_demo --profiles-dir /usr/app/dbt

# 3. Test dbt models
docker-compose -f docker-compose-simple.yml exec dbt dbt test --project-dir /usr/app/dbt/gta_demo --profiles-dir /usr/app/dbt
```

## Setting Up Dashboards in Metabase

1. Go to http://localhost:3000
2. Complete initial setup if not done
3. The PostgreSQL connection is already configured
4. Go to "Browse Data" → you should see:
   - `raw` schema (source data)
   - `staging` schema (cleaned data)
   - `analytics` schema (business models)

5. Create dashboards using queries from `dashboards/metabase_setup.sql`

## Key Tables to Explore

### In `analytics` schema:
- **taxpayer_360_view** - Complete taxpayer profiles with risk scores
- **revenue_dashboard_metrics** - Pre-aggregated revenue metrics
- **fraud_detection_alerts** - ML-based fraud alerts (simulated)

### In `raw` schema:
- **taxpayers** - 50,000 taxpayer records
- **payments** - Payment transactions
- **paye_returns** - PAYE tax returns
- **vat_returns** - VAT returns

## Demo Highlights

1. **Executive Dashboard** - Create using revenue_dashboard_metrics
2. **Taxpayer 360 View** - Search any taxpayer by ID
3. **Geographic Analysis** - Revenue by region/district
4. **Fraud Detection** - High-risk taxpayers and alerts

## Troubleshooting

If you need to restart:
```bash
docker-compose -f docker-compose-simple.yml down
docker-compose -f docker-compose-simple.yml up -d
```

To check logs:
```bash
docker-compose -f docker-compose-simple.yml logs [service-name]
```

## Next Steps

1. Explore the data in Metabase
2. Create custom dashboards
3. Test different visualizations
4. Export reports as needed

The system is fully functional for demonstrating:
- Data warehouse capabilities
- Business intelligence dashboards
- Data transformations with dbt
- Fraud detection scenarios
- Revenue analytics