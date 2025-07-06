#!/usr/bin/env python3
"""
Manual pipeline runner for GTA Data Warehouse
Run this script to execute the data pipeline without Airflow
"""

import os
import sys
import subprocess
import psycopg2
from datetime import datetime

# Add scripts directory to path
sys.path.append('/opt/airflow/scripts' if os.path.exists('/opt/airflow/scripts') else './scripts')

def run_command(cmd, description):
    """Run a shell command and capture output"""
    print(f"\n{'='*60}")
    print(f"Running: {description}")
    print(f"Command: {cmd}")
    print(f"{'='*60}")
    
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    
    if result.returncode == 0:
        print(f"✓ Success: {description}")
        if result.stdout:
            print(result.stdout)
    else:
        print(f"✗ Failed: {description}")
        if result.stderr:
            print(f"Error: {result.stderr}")
        sys.exit(1)
    
    return result

def check_postgres_connection():
    """Check if PostgreSQL is accessible"""
    try:
        conn = psycopg2.connect(
            host="localhost",
            port=5432,
            database="gta_warehouse",
            user="gta_admin",
            password="gta_secure_pass"
        )
        conn.close()
        print("✓ PostgreSQL connection successful")
        return True
    except Exception as e:
        print(f"✗ PostgreSQL connection failed: {e}")
        return False

def main():
    print(f"""
    ╔══════════════════════════════════════════════════════════╗
    ║         GTA Data Warehouse Pipeline Runner               ║
    ║         Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}                    ║
    ╚══════════════════════════════════════════════════════════╝
    """)
    
    # Step 1: Check database connection
    if not check_postgres_connection():
        print("\nPlease ensure PostgreSQL is running:")
        print("docker-compose -f docker-compose-simple.yml up -d postgres")
        sys.exit(1)
    
    # Step 2: Generate synthetic data
    print("\n" + "="*60)
    print("Step 1: Generating Synthetic Data")
    print("="*60)
    
    if os.path.exists('./scripts/generate_synthetic_data.py'):
        run_command(
            "python ./scripts/generate_synthetic_data.py",
            "Generate synthetic Gambian tax data"
        )
    else:
        print("✗ Synthetic data generator not found")
    
    # Step 3: Load data to PostgreSQL
    print("\n" + "="*60)
    print("Step 2: Loading Data to PostgreSQL")
    print("="*60)
    
    # Check if CSV files exist
    if os.path.exists('./data/taxpayers.csv'):
        print("✓ Found generated CSV files in ./data/")
        
        # Load each CSV file
        tables = [
            'taxpayers', 'paye_returns', 'vat_returns', 'payments',
            'companies_registry', 'vehicle_registry', 'land_registry'
        ]
        
        for table in tables:
            csv_file = f'./data/{table}.csv'
            if os.path.exists(csv_file):
                cmd = f"""psql -h localhost -U gta_admin -d gta_warehouse -c "\\copy raw.{table} FROM '{csv_file}' WITH CSV HEADER" """
                run_command(cmd, f"Load {table} data")
    else:
        print("✗ No CSV files found. Please run data generation first.")
    
    # Step 4: Run dbt transformations
    print("\n" + "="*60)
    print("Step 3: Running dbt Transformations")
    print("="*60)
    
    if os.path.exists('./dbt/gta_demo'):
        os.chdir('./dbt')
        run_command(
            "dbt run --project-dir gta_demo --profiles-dir .",
            "Execute dbt models"
        )
        
        run_command(
            "dbt test --project-dir gta_demo --profiles-dir .",
            "Test dbt models"
        )
        os.chdir('..')
    else:
        print("✗ dbt project not found")
    
    print(f"""
    ╔══════════════════════════════════════════════════════════╗
    ║         Pipeline Execution Complete!                     ║
    ║         Finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}                   ║
    ╚══════════════════════════════════════════════════════════╝
    
    Next steps:
    1. Access Metabase at http://localhost:3000
    2. Connect to PostgreSQL and explore the data
    3. Import the dashboard queries from dashboards/metabase_setup.sql
    """)

if __name__ == "__main__":
    main()