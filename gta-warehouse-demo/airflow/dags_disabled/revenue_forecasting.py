"""
Revenue Forecasting Pipeline using Prophet
Generates revenue predictions for GTA planning
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import numpy as np
from prophet import Prophet
import json
import matplotlib.pyplot as plt
import seaborn as sns

default_args = {
    'owner': 'gta_analytics_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email': ['analytics-team@gra.gm'],
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'revenue_forecasting',
    default_args=default_args,
    description='Revenue forecasting using Prophet',
    schedule_interval='0 3 * * 1',  # Run weekly on Mondays at 3 AM
    catchup=False,
    tags=['analytics', 'forecasting', 'weekly']
)

def prepare_revenue_data():
    """Prepare historical revenue data for forecasting"""
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Get daily revenue data
    query = """
        SELECT 
            payment_date as ds,
            tax_type,
            SUM(amount) as y
        FROM raw.payments
        WHERE status = 'Completed'
          AND payment_date >= CURRENT_DATE - INTERVAL '2 years'
        GROUP BY payment_date, tax_type
        ORDER BY payment_date
    """
    
    df = pd.read_sql(query, hook.get_conn())
    
    # Also get aggregated total revenue
    total_revenue = df.groupby('ds')['y'].sum().reset_index()
    total_revenue['tax_type'] = 'Total'
    
    # Combine
    df = pd.concat([df, total_revenue], ignore_index=True)
    
    # Save for forecasting
    df.to_csv('/opt/airflow/data/revenue_history.csv', index=False)
    print(f"Prepared {len(df)} records for forecasting")
    
    return df

def train_revenue_forecasts():
    """Train Prophet models for each tax type"""
    df = pd.read_csv('/opt/airflow/data/revenue_history.csv')
    df['ds'] = pd.to_datetime(df['ds'])
    
    forecasts = {}
    models = {}
    
    for tax_type in df['tax_type'].unique():
        print(f"\nTraining forecast model for {tax_type}...")
        
        # Filter data for this tax type
        tax_df = df[df['tax_type'] == tax_type][['ds', 'y']].copy()
        
        # Create and configure Prophet model
        model = Prophet(
            changepoint_prior_scale=0.05,
            seasonality_mode='multiplicative',
            yearly_seasonality=True,
            weekly_seasonality=True,
            daily_seasonality=False
        )
        
        # Add Gambian holidays and events
        # Ramadan effect (approximate dates, would need exact calendar)
        for year in range(2022, 2025):
            model.add_seasonality(
                name=f'ramadan_{year}',
                period=30,
                fourier_order=5,
                condition_name=f'is_ramadan_{year}'
            )
        
        # Add monthly seasonality for tax deadlines
        model.add_seasonality(name='monthly', period=30.5, fourier_order=5)
        
        # Fit model
        model.fit(tax_df)
        
        # Make predictions
        future = model.make_future_dataframe(periods=90)  # 90 days forecast
        
        # Add Ramadan indicators (simplified)
        for year in range(2022, 2025):
            future[f'is_ramadan_{year}'] = 0  # Would need actual dates
        
        forecast = model.predict(future)
        
        # Store results
        models[tax_type] = model
        forecasts[tax_type] = forecast
        
        # Calculate accuracy metrics on holdout
        holdout_days = 30
        train_df = tax_df[:-holdout_days]
        test_df = tax_df[-holdout_days:]
        
        if len(test_df) > 0:
            model_holdout = Prophet(
                changepoint_prior_scale=0.05,
                seasonality_mode='multiplicative'
            )
            model_holdout.fit(train_df)
            
            future_holdout = model_holdout.make_future_dataframe(periods=holdout_days)
            forecast_holdout = model_holdout.predict(future_holdout)
            
            # Calculate MAPE
            test_forecast = forecast_holdout[forecast_holdout['ds'].isin(test_df['ds'])]
            mape = np.mean(np.abs((test_df['y'].values - test_forecast['yhat'].values) / test_df['y'].values)) * 100
            
            print(f"MAPE for {tax_type}: {mape:.2f}%")
    
    # Save forecasts
    all_forecasts = []
    for tax_type, forecast in forecasts.items():
        forecast_df = forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].copy()
        forecast_df['tax_type'] = tax_type
        all_forecasts.append(forecast_df)
    
    combined_forecasts = pd.concat(all_forecasts, ignore_index=True)
    combined_forecasts.to_csv('/opt/airflow/data/revenue_forecasts.csv', index=False)
    
    # Generate forecast summary
    generate_forecast_summary(forecasts)
    
    print("Revenue forecasting complete!")

def generate_forecast_summary(forecasts):
    """Generate summary statistics and visualizations"""
    summary = {}
    
    for tax_type, forecast in forecasts.items():
        # Get next 30 days forecast
        future_forecast = forecast[forecast['ds'] > pd.Timestamp.now()].head(30)
        
        if len(future_forecast) > 0:
            summary[tax_type] = {
                'next_30_days_total': future_forecast['yhat'].sum(),
                'next_30_days_avg': future_forecast['yhat'].mean(),
                'next_30_days_min': future_forecast['yhat_lower'].min(),
                'next_30_days_max': future_forecast['yhat_upper'].max(),
                'trend_direction': 'increasing' if future_forecast['yhat'].iloc[-1] > future_forecast['yhat'].iloc[0] else 'decreasing'
            }
    
    # Save summary
    with open('/opt/airflow/data/forecast_summary.json', 'w') as f:
        json.dump(summary, f, indent=2, default=str)

def update_forecast_tables():
    """Update database with latest forecasts"""
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Read forecasts
    forecasts_df = pd.read_csv('/opt/airflow/data/revenue_forecasts.csv')
    forecasts_df['ds'] = pd.to_datetime(forecasts_df['ds'])
    
    # Create forecast table if not exists
    create_table = """
        CREATE TABLE IF NOT EXISTS analytics.revenue_forecasts (
            forecast_date DATE,
            tax_type VARCHAR(50),
            predicted_revenue DECIMAL(15,2),
            lower_bound DECIMAL(15,2),
            upper_bound DECIMAL(15,2),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (forecast_date, tax_type)
        )
    """
    hook.run(create_table)
    
    # Clear existing forecasts
    hook.run("TRUNCATE TABLE analytics.revenue_forecasts")
    
    # Insert new forecasts
    conn = hook.get_conn()
    cursor = conn.cursor()
    
    for _, row in forecasts_df.iterrows():
        insert_query = """
            INSERT INTO analytics.revenue_forecasts 
            (forecast_date, tax_type, predicted_revenue, lower_bound, upper_bound)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (forecast_date, tax_type) 
            DO UPDATE SET 
                predicted_revenue = EXCLUDED.predicted_revenue,
                lower_bound = EXCLUDED.lower_bound,
                upper_bound = EXCLUDED.upper_bound,
                created_at = CURRENT_TIMESTAMP
        """
        
        cursor.execute(insert_query, (
            row['ds'].date(),
            row['tax_type'],
            round(row['yhat'], 2),
            round(row['yhat_lower'], 2),
            round(row['yhat_upper'], 2)
        ))
    
    conn.commit()
    cursor.close()
    conn.close()
    
    print(f"Updated {len(forecasts_df)} forecast records")

def generate_forecast_alerts():
    """Generate alerts for significant forecast deviations"""
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Check for significant drops in forecasted revenue
    query = """
        WITH forecast_comparison AS (
            SELECT 
                f1.tax_type,
                f1.forecast_date,
                f1.predicted_revenue as current_forecast,
                AVG(p.amount) as historical_avg,
                (f1.predicted_revenue - AVG(p.amount)) / NULLIF(AVG(p.amount), 0) * 100 as variance_pct
            FROM analytics.revenue_forecasts f1
            JOIN raw.payments p ON p.tax_type = f1.tax_type
                AND p.payment_date >= CURRENT_DATE - INTERVAL '30 days'
                AND p.payment_date < CURRENT_DATE
            WHERE f1.forecast_date BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL '7 days'
            GROUP BY f1.tax_type, f1.forecast_date, f1.predicted_revenue
        )
        SELECT * FROM forecast_comparison
        WHERE ABS(variance_pct) > 20
    """
    
    alerts = pd.read_sql(query, hook.get_conn())
    
    if len(alerts) > 0:
        print(f"\nRevenue Forecast Alerts:")
        for _, alert in alerts.iterrows():
            direction = "decrease" if alert['variance_pct'] < 0 else "increase"
            print(f"- {alert['tax_type']}: Expecting {abs(alert['variance_pct']):.1f}% {direction} on {alert['forecast_date']}")
    
    # Save alerts
    alerts.to_csv('/opt/airflow/data/forecast_alerts.csv', index=False)

# Define tasks
task_prepare_data = PythonOperator(
    task_id='prepare_revenue_data',
    python_callable=prepare_revenue_data,
    dag=dag
)

task_train_forecasts = PythonOperator(
    task_id='train_revenue_forecasts',
    python_callable=train_revenue_forecasts,
    dag=dag
)

task_update_tables = PythonOperator(
    task_id='update_forecast_tables',
    python_callable=update_forecast_tables,
    dag=dag
)

task_generate_alerts = PythonOperator(
    task_id='generate_forecast_alerts',
    python_callable=generate_forecast_alerts,
    dag=dag
)

# Define dependencies
task_prepare_data >> task_train_forecasts >> task_update_tables >> task_generate_alerts