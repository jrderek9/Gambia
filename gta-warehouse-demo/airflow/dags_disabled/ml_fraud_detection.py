"""
Machine Learning Fraud Detection Pipeline
Trains and deploys fraud detection models for GTA
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import classification_report, roc_auc_score
import xgboost as xgb
import joblib
import json
import os

default_args = {
    'owner': 'gta_ml_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email': ['ml-team@gra.gm'],
    'retries': 1,
    'retry_delay': timedelta(minutes=10)
}

dag = DAG(
    'ml_fraud_detection',
    default_args=default_args,
    description='ML pipeline for fraud detection',
    schedule_interval='0 4 * * 1',  # Run weekly on Mondays at 4 AM
    catchup=False,
    tags=['ml', 'fraud-detection', 'weekly']
)

def prepare_fraud_features():
    """Extract and engineer features for fraud detection"""
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Get taxpayer behavior data
    query = """
        WITH taxpayer_features AS (
            SELECT 
                t.taxpayer_id,
                t.taxpayer_type,
                t.business_sector,
                t.annual_turnover,
                t.employee_count,
                t.compliance_score,
                t.risk_category,
                DATE_PART('year', AGE(CURRENT_DATE, t.registration_date)) as years_active,
                
                -- Filing behavior
                COUNT(DISTINCT pr.return_id) as paye_returns_count,
                COUNT(DISTINCT vr.return_id) as vat_returns_count,
                AVG(CASE WHEN pr.filing_date > pr.due_date THEN 1 ELSE 0 END) as paye_late_rate,
                AVG(CASE WHEN vr.filing_date > vr.due_date THEN 1 ELSE 0 END) as vat_late_rate,
                
                -- Payment behavior
                COUNT(DISTINCT p.payment_id) as payment_count,
                AVG(p.amount) as avg_payment_amount,
                STDDEV(p.amount) as payment_amount_stddev,
                COUNT(DISTINCT p.payment_channel) as payment_channels_used,
                
                -- Revenue patterns
                COALESCE(AVG(vr.total_sales), 0) as avg_vat_sales,
                COALESCE(STDDEV(vr.total_sales), 0) as vat_sales_stddev,
                COALESCE(MIN(vr.total_sales), 0) as min_vat_sales,
                COALESCE(MAX(vr.total_sales), 0) as max_vat_sales,
                
                -- Fraud indicator (target variable)
                CASE WHEN t.risk_category = 'High' AND t.compliance_score < 0.5 THEN 1 ELSE 0 END as is_fraud
                
            FROM raw.taxpayers t
            LEFT JOIN raw.paye_returns pr ON t.taxpayer_id = pr.taxpayer_id
            LEFT JOIN raw.vat_returns vr ON t.taxpayer_id = vr.taxpayer_id
            LEFT JOIN raw.payments p ON t.taxpayer_id = p.taxpayer_id
            WHERE t.taxpayer_type IN ('Corporate', 'Partnership')
            GROUP BY t.taxpayer_id, t.taxpayer_type, t.business_sector, 
                     t.annual_turnover, t.employee_count, t.compliance_score, 
                     t.risk_category, t.registration_date
        )
        SELECT * FROM taxpayer_features
        WHERE paye_returns_count > 0 OR vat_returns_count > 0
    """
    
    df = pd.read_sql(query, hook.get_conn())
    
    # Additional feature engineering
    df['payment_consistency'] = df['avg_payment_amount'] / (df['payment_amount_stddev'] + 1)
    df['vat_sales_volatility'] = df['vat_sales_stddev'] / (df['avg_vat_sales'] + 1)
    df['filing_reliability'] = 1 - ((df['paye_late_rate'] + df['vat_late_rate']) / 2)
    df['returns_per_year'] = (df['paye_returns_count'] + df['vat_returns_count']) / (df['years_active'] + 1)
    
    # Handle missing values
    df = df.fillna(0)
    
    # Save features
    df.to_csv('/opt/airflow/data/fraud_features.csv', index=False)
    print(f"Prepared {len(df)} records with {len(df.columns)} features")
    
    return df

def train_fraud_models():
    """Train multiple fraud detection models"""
    # Load features
    df = pd.read_csv('/opt/airflow/data/fraud_features.csv')
    
    # Prepare features and target
    feature_cols = [col for col in df.columns if col not in ['taxpayer_id', 'is_fraud', 'taxpayer_type', 'business_sector']]
    X = df[feature_cols]
    y = df['is_fraud']
    
    # Handle categorical variables
    X = pd.get_dummies(X, columns=[], drop_first=True)
    
    # Split data
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)
    
    # Scale features
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)
    
    # Train models
    models = {
        'random_forest': RandomForestClassifier(n_estimators=100, random_state=42, class_weight='balanced'),
        'gradient_boosting': GradientBoostingClassifier(n_estimators=100, random_state=42),
        'xgboost': xgb.XGBClassifier(n_estimators=100, random_state=42, scale_pos_weight=len(y_train[y_train==0])/len(y_train[y_train==1]))
    }
    
    results = {}
    best_model = None
    best_score = 0
    
    for name, model in models.items():
        print(f"\nTraining {name}...")
        
        # Train model
        model.fit(X_train_scaled, y_train)
        
        # Evaluate
        y_pred = model.predict(X_test_scaled)
        y_pred_proba = model.predict_proba(X_test_scaled)[:, 1]
        
        # Calculate metrics
        roc_score = roc_auc_score(y_test, y_pred_proba)
        cv_scores = cross_val_score(model, X_train_scaled, y_train, cv=5, scoring='roc_auc')
        
        results[name] = {
            'roc_auc': roc_score,
            'cv_mean': cv_scores.mean(),
            'cv_std': cv_scores.std(),
            'classification_report': classification_report(y_test, y_pred, output_dict=True)
        }
        
        print(f"ROC-AUC: {roc_score:.4f}")
        print(f"CV Score: {cv_scores.mean():.4f} (+/- {cv_scores.std():.4f})")
        
        # Track best model
        if roc_score > best_score:
            best_score = roc_score
            best_model = (name, model)
    
    # Save best model
    os.makedirs('/opt/airflow/models', exist_ok=True)
    joblib.dump(best_model[1], '/opt/airflow/models/fraud_detection_model.pkl')
    joblib.dump(scaler, '/opt/airflow/models/fraud_detection_scaler.pkl')
    joblib.dump(feature_cols, '/opt/airflow/models/fraud_detection_features.pkl')
    
    # Save results
    with open('/opt/airflow/models/model_performance.json', 'w') as f:
        json.dump(results, f, indent=2)
    
    print(f"\nBest model: {best_model[0]} with ROC-AUC: {best_score:.4f}")
    
    # Feature importance for best model
    if hasattr(best_model[1], 'feature_importances_'):
        importance_df = pd.DataFrame({
            'feature': feature_cols,
            'importance': best_model[1].feature_importances_
        }).sort_values('importance', ascending=False).head(15)
        
        importance_df.to_csv('/opt/airflow/models/feature_importance.csv', index=False)
        print("\nTop 15 Important Features:")
        print(importance_df)

def score_all_taxpayers():
    """Score all taxpayers using the trained model"""
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Load model and scaler
    model = joblib.load('/opt/airflow/models/fraud_detection_model.pkl')
    scaler = joblib.load('/opt/airflow/models/fraud_detection_scaler.pkl')
    feature_cols = joblib.load('/opt/airflow/models/fraud_detection_features.pkl')
    
    # Get all taxpayer features
    df = prepare_fraud_features()
    
    # Prepare features
    X = df[feature_cols].fillna(0)
    X_scaled = scaler.transform(X)
    
    # Generate predictions
    fraud_probabilities = model.predict_proba(X_scaled)[:, 1]
    
    # Add predictions to dataframe
    df['fraud_probability'] = fraud_probabilities
    df['fraud_risk_category'] = pd.cut(
        fraud_probabilities,
        bins=[0, 0.3, 0.6, 0.8, 1.0],
        labels=['Low', 'Medium', 'High', 'Critical']
    )
    
    # Update risk scores in database
    conn = hook.get_conn()
    cursor = conn.cursor()
    
    for _, row in df.iterrows():
        update_query = """
            UPDATE raw.taxpayers
            SET risk_category = %s,
                compliance_score = %s,
                updated_at = CURRENT_TIMESTAMP
            WHERE taxpayer_id = %s
        """
        
        # Adjust compliance score based on fraud probability
        new_compliance_score = max(0.1, min(0.95, 1 - row['fraud_probability']))
        
        cursor.execute(update_query, (
            row['fraud_risk_category'],
            new_compliance_score,
            row['taxpayer_id']
        ))
    
    conn.commit()
    cursor.close()
    conn.close()
    
    # Save detailed scores
    df[['taxpayer_id', 'fraud_probability', 'fraud_risk_category']].to_csv(
        '/opt/airflow/data/fraud_scores.csv', index=False
    )
    
    print(f"Scored {len(df)} taxpayers")
    print(f"Risk distribution:\n{df['fraud_risk_category'].value_counts()}")

def generate_fraud_alerts():
    """Generate actionable fraud alerts based on model predictions"""
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Load fraud scores
    scores_df = pd.read_csv('/opt/airflow/data/fraud_scores.csv')
    high_risk = scores_df[scores_df['fraud_probability'] > 0.7]
    
    # Clear existing open alerts
    hook.run("UPDATE analytics.fraud_alerts SET status = 'Closed' WHERE status = 'Open'")
    
    # Generate new alerts for high-risk taxpayers
    conn = hook.get_conn()
    cursor = conn.cursor()
    
    for _, taxpayer in high_risk.iterrows():
        # Get additional context
        context_query = """
            SELECT 
                t.name,
                t.business_sector,
                COUNT(DISTINCT pr.return_id) as missing_paye_returns,
                COUNT(DISTINCT vr.return_id) as missing_vat_returns,
                SUM(p.amount) as recent_payments
            FROM raw.taxpayers t
            LEFT JOIN raw.paye_returns pr ON t.taxpayer_id = pr.taxpayer_id 
                AND pr.status = 'Overdue'
                AND pr.due_date >= CURRENT_DATE - INTERVAL '6 months'
            LEFT JOIN raw.vat_returns vr ON t.taxpayer_id = vr.taxpayer_id 
                AND vr.status = 'Overdue'
                AND vr.due_date >= CURRENT_DATE - INTERVAL '6 months'
            LEFT JOIN raw.payments p ON t.taxpayer_id = p.taxpayer_id
                AND p.payment_date >= CURRENT_DATE - INTERVAL '3 months'
            WHERE t.taxpayer_id = %s
            GROUP BY t.name, t.business_sector
        """
        
        cursor.execute(context_query, (taxpayer['taxpayer_id'],))
        context = cursor.fetchone()
        
        if context:
            alert_type = "ML-Detected High Fraud Risk"
            description = f"Fraud probability: {taxpayer['fraud_probability']:.2%}. "
            
            if context[2] > 0:  # missing PAYE returns
                description += f"{context[2]} missing PAYE returns. "
            if context[3] > 0:  # missing VAT returns
                description += f"{context[3]} missing VAT returns. "
            
            insert_alert = """
                INSERT INTO analytics.fraud_alerts 
                (taxpayer_id, alert_date, alert_type, risk_score, description, status)
                VALUES (%s, %s, %s, %s, %s, %s)
            """
            
            cursor.execute(insert_alert, (
                taxpayer['taxpayer_id'],
                datetime.now().date(),
                alert_type,
                taxpayer['fraud_probability'],
                description,
                'Open'
            ))
    
    conn.commit()
    cursor.close()
    conn.close()
    
    print(f"Generated {len(high_risk)} fraud alerts")

# Define tasks
task_prepare_features = PythonOperator(
    task_id='prepare_fraud_features',
    python_callable=prepare_fraud_features,
    dag=dag
)

task_train_models = PythonOperator(
    task_id='train_fraud_models',
    python_callable=train_fraud_models,
    dag=dag
)

task_score_taxpayers = PythonOperator(
    task_id='score_all_taxpayers',
    python_callable=score_all_taxpayers,
    dag=dag
)

task_generate_alerts = PythonOperator(
    task_id='generate_fraud_alerts',
    python_callable=generate_fraud_alerts,
    dag=dag
)

# Define dependencies
task_prepare_features >> task_train_models >> task_score_taxpayers >> task_generate_alerts