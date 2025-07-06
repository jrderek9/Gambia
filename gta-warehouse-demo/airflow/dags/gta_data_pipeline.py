"""
Main data pipeline DAG for GTA Data Warehouse
Orchestrates data ingestion, transformation, and analytics
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
import pandas as pd
import sys
sys.path.append('/opt/airflow/scripts')

default_args = {
    'owner': 'gta_data_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'email': ['data-team@gra.gm'],
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'gta_data_pipeline',
    default_args=default_args,
    description='Main ETL pipeline for GTA data warehouse',
    schedule_interval='0 2 * * *',  # Run daily at 2 AM
    catchup=False,
    tags=['production', 'etl', 'daily']
)

def check_data_quality():
    """Run data quality checks on source data"""
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Check for NULL taxpayer IDs
    null_check = """
        SELECT COUNT(*) as null_count
        FROM raw.taxpayers
        WHERE taxpayer_id IS NULL OR tin IS NULL
    """
    result = hook.get_first(null_check)
    if result[0] > 0:
        raise ValueError(f"Found {result[0]} records with NULL taxpayer_id or tin")
    
    # Check for duplicate TINs
    duplicate_check = """
        SELECT tin, COUNT(*) as count
        FROM raw.taxpayers
        GROUP BY tin
        HAVING COUNT(*) > 1
    """
    duplicates = hook.get_records(duplicate_check)
    if duplicates:
        raise ValueError(f"Found duplicate TINs: {duplicates}")
    
    print("Data quality checks passed!")

def generate_synthetic_data():
    """Generate fresh synthetic data for demo"""
    from generate_synthetic_data import GambianTaxDataGenerator
    
    print("Generating synthetic data...")
    generator = GambianTaxDataGenerator(num_taxpayers=50000)
    all_data = generator.generate_all_data()
    
    # Save to CSV for loading
    generator.save_to_csv(all_data, output_dir='/opt/airflow/data')
    print("Synthetic data generation complete!")

def load_data_to_postgres():
    """Load CSV data into PostgreSQL"""
    hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = hook.get_conn()
    cursor = conn.cursor()
    
    # Define tables and their CSV files
    tables = {
        'raw.taxpayers': 'taxpayers.csv',
        'raw.paye_returns': 'paye_returns.csv',
        'raw.vat_returns': 'vat_returns.csv',
        'raw.payments': 'payments.csv',
        'raw.companies_registry': 'companies_registry.csv',
        'raw.vehicle_registry': 'vehicle_registry.csv',
        'raw.land_registry': 'land_registry.csv',
        'analytics.fraud_alerts': 'fraud_alerts.csv'
    }
    
    for table, csv_file in tables.items():
        print(f"Loading data into {table}...")
        
        # Clear existing data
        cursor.execute(f"TRUNCATE TABLE {table} CASCADE")
        
        # Load new data
        df = pd.read_csv(f'/opt/airflow/data/{csv_file}')
        
        # Handle date conversions
        date_columns = ['registration_date', 'filing_date', 'due_date', 'payment_date', 
                       'incorporation_date', 'last_filing_date', 'purchase_date', 
                       'acquisition_date', 'alert_date', 'created_at', 'updated_at']
        
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        
        # Insert data
        for _, row in df.iterrows():
            columns = ', '.join(row.index)
            placeholders = ', '.join(['%s'] * len(row))
            insert_query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
            
            # Convert NaN to None for SQL NULL
            values = [None if pd.isna(val) else val for val in row.values]
            cursor.execute(insert_query, values)
        
        conn.commit()
        print(f"Loaded {len(df)} records into {table}")
    
    cursor.close()
    conn.close()

def calculate_metrics():
    """Calculate additional metrics and aggregations"""
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Update compliance scores based on actual behavior
    update_compliance = """
        UPDATE raw.taxpayers t
        SET compliance_score = subq.new_score,
            updated_at = CURRENT_TIMESTAMP
        FROM (
            SELECT 
                t.taxpayer_id,
                GREATEST(0.1, LEAST(0.95, 
                    (COALESCE(paye_filed.count, 0) + COALESCE(vat_filed.count, 0))::NUMERIC / 
                    NULLIF(COALESCE(paye_expected.count, 0) + COALESCE(vat_expected.count, 0), 0)
                )) as new_score
            FROM raw.taxpayers t
            LEFT JOIN (
                SELECT taxpayer_id, COUNT(*) as count
                FROM raw.paye_returns
                WHERE status = 'Filed'
                GROUP BY taxpayer_id
            ) paye_filed ON t.taxpayer_id = paye_filed.taxpayer_id
            LEFT JOIN (
                SELECT taxpayer_id, COUNT(*) as count
                FROM raw.paye_returns
                GROUP BY taxpayer_id
            ) paye_expected ON t.taxpayer_id = paye_expected.taxpayer_id
            LEFT JOIN (
                SELECT taxpayer_id, COUNT(*) as count
                FROM raw.vat_returns
                WHERE status = 'Filed'
                GROUP BY taxpayer_id
            ) vat_filed ON t.taxpayer_id = vat_filed.taxpayer_id
            LEFT JOIN (
                SELECT taxpayer_id, COUNT(*) as count
                FROM raw.vat_returns
                GROUP BY taxpayer_id
            ) vat_expected ON t.taxpayer_id = vat_expected.taxpayer_id
        ) subq
        WHERE t.taxpayer_id = subq.taxpayer_id
    """
    
    hook.run(update_compliance)
    print("Metrics calculation complete!")

# Define tasks
task_generate_data = PythonOperator(
    task_id='generate_synthetic_data',
    python_callable=generate_synthetic_data,
    dag=dag
)

task_load_data = PythonOperator(
    task_id='load_data_to_postgres',
    python_callable=load_data_to_postgres,
    dag=dag
)

task_quality_check = PythonOperator(
    task_id='data_quality_checks',
    python_callable=check_data_quality,
    dag=dag
)

task_run_dbt = BashOperator(
    task_id='run_dbt_models',
    bash_command='cd /usr/app/dbt && dbt run --project-dir gta_demo --profiles-dir .',
    dag=dag
)

task_test_dbt = BashOperator(
    task_id='test_dbt_models',
    bash_command='cd /usr/app/dbt && dbt test --project-dir gta_demo --profiles-dir .',
    dag=dag
)

task_calculate_metrics = PythonOperator(
    task_id='calculate_metrics',
    python_callable=calculate_metrics,
    dag=dag
)

# Define task dependencies
task_generate_data >> task_load_data >> task_quality_check >> task_run_dbt >> task_test_dbt >> task_calculate_metrics