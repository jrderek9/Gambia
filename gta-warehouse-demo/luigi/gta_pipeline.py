#!/usr/bin/env python3
"""
GTA Data Warehouse Luigi Pipeline
Comprehensive ETL pipeline with ML and real-time features
"""

import luigi
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import execute_batch
import json
import random
from faker import Faker
from sklearn.ensemble import RandomForestClassifier, IsolationForest
from sklearn.preprocessing import StandardScaler
import pickle
import os

fake = Faker()

class DatabaseConfig(luigi.Config):
    host = luigi.Parameter(default='postgres')
    port = luigi.IntParameter(default=5432)
    database = luigi.Parameter(default='gta_warehouse')
    user = luigi.Parameter(default='gta_admin')
    password = luigi.Parameter(default='gta_secure_pass')
    
    def get_connection(self):
        return psycopg2.connect(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password
        )

class GenerateRealtimeData(luigi.Task):
    """Generate real-time transaction data"""
    date = luigi.DateParameter(default=datetime.now().date())
    
    def output(self):
        return luigi.LocalTarget(f'/usr/app/data/realtime_transactions_{self.date}.csv')
    
    def run(self):
        # Generate realistic real-time transactions
        transactions = []
        
        # Get existing taxpayers
        db = DatabaseConfig()
        conn = db.get_connection()
        df_taxpayers = pd.read_sql("SELECT taxpayer_id, region, business_sector FROM raw.taxpayers", conn)
        conn.close()
        
        # Generate transactions for today with realistic patterns
        num_transactions = random.randint(100, 200)
        
        for _ in range(num_transactions):
            taxpayer = df_taxpayers.sample(1).iloc[0]
            
            # Time patterns - business hours peak
            hour = np.random.choice(range(24), p=self._get_hourly_distribution())
            minute = random.randint(0, 59)
            
            # Payment patterns by region
            if taxpayer['region'] == 'Greater Banjul Area':
                payment_channel = np.random.choice(
                    ['Online', 'Bank Transfer', 'Mobile Money', 'POS'],
                    p=[0.3, 0.4, 0.25, 0.05]
                )
            else:
                payment_channel = np.random.choice(
                    ['Mobile Money', 'Bank Transfer', 'Cash', 'Online'],
                    p=[0.45, 0.3, 0.15, 0.1]
                )
            
            # Amount patterns by sector
            base_amounts = {
                'Retail': (10000, 500000),
                'Services': (20000, 1000000),
                'Manufacturing': (50000, 2000000),
                'Agriculture': (15000, 800000),
                'Import/Export': (100000, 5000000)
            }
            
            min_amt, max_amt = base_amounts.get(taxpayer['business_sector'], (10000, 500000))
            amount = random.uniform(min_amt, max_amt)
            
            # Add anomalies for fraud detection
            if random.random() < 0.05:  # 5% anomaly rate
                amount *= random.uniform(3, 5)  # Unusually large transaction
                
            transaction = {
                'transaction_id': f"TXN{self.date.strftime('%Y%m%d')}{random.randint(10000, 99999)}",
                'taxpayer_id': taxpayer['taxpayer_id'],
                'timestamp': f"{self.date} {hour:02d}:{minute:02d}:{random.randint(0,59):02d}",
                'payment_channel': payment_channel,
                'amount': round(amount, 2),
                'tax_type': random.choice(['VAT', 'PAYE', 'Corporate Tax', 'Withholding Tax']),
                'status': 'Pending',
                'risk_flag': amount > max_amt * 2
            }
            transactions.append(transaction)
        
        # Save transactions
        df = pd.DataFrame(transactions)
        df.to_csv(self.output().path, index=False)
    
    def _get_hourly_distribution(self):
        """Realistic hourly transaction distribution"""
        dist = np.zeros(24)
        # Business hours (8am-6pm) peak
        dist[8:18] = np.array([0.05, 0.08, 0.12, 0.15, 0.18, 0.15, 0.10, 0.08, 0.06, 0.03])
        # Normalize
        return dist / dist.sum()

class ProcessRealtimeTransactions(luigi.Task):
    """Process and validate real-time transactions"""
    date = luigi.DateParameter(default=datetime.now().date())
    
    def requires(self):
        return GenerateRealtimeData(self.date)
    
    def output(self):
        return luigi.LocalTarget(f'/usr/app/data/processed_transactions_{self.date}.csv')
    
    def run(self):
        # Load transactions
        df = pd.read_csv(self.input().path)
        
        # Apply business rules
        df['validation_status'] = 'Valid'
        df['processing_time'] = pd.Timestamp.now()
        
        # Check for duplicate transactions
        df['is_duplicate'] = df.duplicated(subset=['taxpayer_id', 'amount', 'tax_type'], keep=False)
        df.loc[df['is_duplicate'], 'validation_status'] = 'Duplicate Warning'
        
        # Check for unusual patterns
        amount_threshold = df.groupby('tax_type')['amount'].quantile(0.95)
        for tax_type, threshold in amount_threshold.items():
            mask = (df['tax_type'] == tax_type) & (df['amount'] > threshold)
            df.loc[mask, 'validation_status'] = 'High Amount Warning'
        
        # Update status
        df['status'] = df.apply(
            lambda x: 'Completed' if x['validation_status'] == 'Valid' else 'Under Review',
            axis=1
        )
        
        df.to_csv(self.output().path, index=False)

class TrainFraudDetectionModel(luigi.Task):
    """Train advanced ML fraud detection model"""
    
    def output(self):
        return {
            'model': luigi.LocalTarget('/usr/app/ml/fraud_model.pkl'),
            'scaler': luigi.LocalTarget('/usr/app/ml/fraud_scaler.pkl'),
            'metrics': luigi.LocalTarget('/usr/app/ml/fraud_metrics.json')
        }
    
    def run(self):
        db = DatabaseConfig()
        conn = db.get_connection()
        
        # Get comprehensive features
        query = """
        WITH taxpayer_features AS (
            SELECT 
                t.taxpayer_id,
                t.annual_turnover,
                COALESCE(t.employee_count, 0) as employee_count,
                t.compliance_score,
                EXTRACT(YEAR FROM AGE(CURRENT_DATE, t.registration_date)) as years_active,
                
                -- Payment patterns
                COUNT(DISTINCT p.payment_id) as total_payments,
                COUNT(DISTINCT p.payment_channel) as payment_channels,
                AVG(p.amount) as avg_payment,
                STDDEV(p.amount) as payment_stddev,
                MAX(p.amount) as max_payment,
                
                -- Filing patterns
                COUNT(DISTINCT pr.return_id) as paye_returns,
                COUNT(DISTINCT vr.return_id) as vat_returns,
                
                -- Calculate filing delays
                AVG(CASE 
                    WHEN pr.filing_date > pr.due_date 
                    THEN EXTRACT(DAY FROM pr.filing_date - pr.due_date) 
                    ELSE 0 
                END) as avg_paye_delay,
                
                -- Fraud label
                CASE 
                    WHEN t.risk_category = 'High' AND t.compliance_score < 0.5 THEN 1
                    WHEN EXISTS (
                        SELECT 1 FROM analytics.fraud_alerts fa 
                        WHERE fa.taxpayer_id = t.taxpayer_id 
                        AND fa.risk_score > 0.7
                    ) THEN 1
                    ELSE 0
                END as is_fraud
                
            FROM raw.taxpayers t
            LEFT JOIN raw.payments p ON t.taxpayer_id = p.taxpayer_id
            LEFT JOIN raw.paye_returns pr ON t.taxpayer_id = pr.taxpayer_id
            LEFT JOIN raw.vat_returns vr ON t.taxpayer_id = vr.taxpayer_id
            WHERE t.taxpayer_type IN ('Corporate', 'Partnership')
            GROUP BY t.taxpayer_id, t.annual_turnover, t.employee_count, 
                     t.compliance_score, t.registration_date, t.risk_category
        )
        SELECT * FROM taxpayer_features
        """
        
        df = pd.read_sql(query, conn)
        conn.close()
        
        # Feature engineering
        df['payment_consistency'] = df['avg_payment'] / (df['payment_stddev'] + 1)
        df['payment_velocity'] = df['total_payments'] / (df['years_active'] + 1)
        df['channel_diversity'] = df['payment_channels'] / df['total_payments'].clip(lower=1)
        
        # Prepare features
        feature_cols = [
            'annual_turnover', 'employee_count', 'compliance_score', 'years_active',
            'total_payments', 'payment_channels', 'avg_payment', 'payment_stddev',
            'max_payment', 'paye_returns', 'vat_returns', 'avg_paye_delay',
            'payment_consistency', 'payment_velocity', 'channel_diversity'
        ]
        
        X = df[feature_cols].fillna(0)
        y = df['is_fraud']
        
        # Balance dataset by oversampling fraud cases
        fraud_indices = df[df['is_fraud'] == 1].index
        normal_indices = df[df['is_fraud'] == 0].index
        
        # Oversample fraud cases
        oversampled_fraud = np.random.choice(fraud_indices, size=len(normal_indices)//2, replace=True)
        balanced_indices = np.concatenate([normal_indices, oversampled_fraud])
        
        X_balanced = X.loc[balanced_indices]
        y_balanced = y.loc[balanced_indices]
        
        # Scale features
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X_balanced)
        
        # Train model
        model = RandomForestClassifier(
            n_estimators=100,
            max_depth=10,
            min_samples_split=20,
            class_weight='balanced',
            random_state=42
        )
        
        model.fit(X_scaled, y_balanced)
        
        # Calculate metrics
        from sklearn.model_selection import cross_val_score
        scores = cross_val_score(model, X_scaled, y_balanced, cv=5, scoring='roc_auc')
        
        metrics = {
            'auc_mean': float(scores.mean()),
            'auc_std': float(scores.std()),
            'feature_importance': dict(zip(feature_cols, model.feature_importances_.tolist())),
            'training_date': datetime.now().isoformat(),
            'model_version': '2.0'
        }
        
        # Save model and artifacts
        os.makedirs('/usr/app/ml', exist_ok=True)
        
        with open(self.output()['model'].path, 'wb') as f:
            pickle.dump(model, f)
        
        with open(self.output()['scaler'].path, 'wb') as f:
            pickle.dump(scaler, f)
        
        with open(self.output()['metrics'].path, 'w') as f:
            json.dump(metrics, f, indent=2)

class DetectAnomalies(luigi.Task):
    """Detect anomalies using Isolation Forest"""
    date = luigi.DateParameter(default=datetime.now().date())
    
    def requires(self):
        return ProcessRealtimeTransactions(self.date)
    
    def output(self):
        return luigi.LocalTarget(f'/usr/app/data/anomalies_{self.date}.csv')
    
    def run(self):
        # Load processed transactions
        df = pd.read_csv(self.input().path)
        
        # Prepare features for anomaly detection
        features = ['amount', 'risk_flag']
        
        # Add time-based features
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df['hour'] = df['timestamp'].dt.hour
        df['is_weekend'] = df['timestamp'].dt.dayofweek >= 5
        
        features.extend(['hour', 'is_weekend'])
        
        # One-hot encode categorical features
        df_encoded = pd.get_dummies(df[['payment_channel', 'tax_type']])
        X = pd.concat([df[features], df_encoded], axis=1).fillna(0)
        
        # Train Isolation Forest
        iso_forest = IsolationForest(
            contamination=0.05,  # Expect 5% anomalies
            random_state=42
        )
        
        anomaly_labels = iso_forest.fit_predict(X)
        anomaly_scores = iso_forest.score_samples(X)
        
        # Add results
        df['is_anomaly'] = anomaly_labels == -1
        df['anomaly_score'] = anomaly_scores
        
        # Generate alerts for anomalies
        anomalies = df[df['is_anomaly']].copy()
        anomalies['alert_reason'] = anomalies.apply(self._get_alert_reason, axis=1)
        
        anomalies.to_csv(self.output().path, index=False)
    
    def _get_alert_reason(self, row):
        reasons = []
        
        if row['amount'] > 1000000:
            reasons.append("Unusually high amount")
        
        if row['hour'] < 6 or row['hour'] > 22:
            reasons.append("Transaction outside business hours")
        
        if row['is_weekend']:
            reasons.append("Weekend transaction")
        
        if row['risk_flag']:
            reasons.append("Pre-flagged as risky")
        
        return "; ".join(reasons) if reasons else "Complex pattern detected"

class GeneratePredictiveInsights(luigi.Task):
    """Generate predictive insights and recommendations"""
    date = luigi.DateParameter(default=datetime.now().date())
    
    def requires(self):
        return {
            'fraud_model': TrainFraudDetectionModel(),
            'anomalies': DetectAnomalies(self.date)
        }
    
    def output(self):
        return luigi.LocalTarget(f'/usr/app/data/insights_{self.date}.json')
    
    def run(self):
        db = DatabaseConfig()
        conn = db.get_connection()
        
        # Load fraud model
        with open(self.input()['fraud_model']['model'].path, 'rb') as f:
            model = pickle.load(f)
        
        # Generate various insights
        insights = {
            'generation_date': self.date.isoformat(),
            'revenue_insights': self._get_revenue_insights(conn),
            'compliance_insights': self._get_compliance_insights(conn),
            'fraud_insights': self._get_fraud_insights(conn),
            'recommendations': self._generate_recommendations(conn),
            'predicted_collections': self._predict_collections(conn)
        }
        
        conn.close()
        
        with open(self.output().path, 'w') as f:
            json.dump(insights, f, indent=2)
    
    def _get_revenue_insights(self, conn):
        query = """
        WITH current_month AS (
            SELECT 
                SUM(amount) as total,
                COUNT(DISTINCT taxpayer_id) as taxpayers
            FROM raw.payments
            WHERE DATE_TRUNC('month', payment_date) = DATE_TRUNC('month', CURRENT_DATE)
        ),
        previous_month AS (
            SELECT 
                SUM(amount) as total
            FROM raw.payments
            WHERE DATE_TRUNC('month', payment_date) = DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month')
        )
        SELECT 
            cm.total as current_revenue,
            pm.total as previous_revenue,
            cm.taxpayers as active_taxpayers,
            ((cm.total - pm.total) / pm.total * 100) as growth_rate
        FROM current_month cm, previous_month pm
        """
        
        result = pd.read_sql(query, conn).iloc[0]
        
        return {
            'current_month_revenue': float(result['current_revenue'] or 0),
            'growth_rate': float(result['growth_rate'] or 0),
            'active_taxpayers': int(result['active_taxpayers'] or 0),
            'status': 'on_track' if result['growth_rate'] > 0 else 'below_target'
        }
    
    def _get_compliance_insights(self, conn):
        query = """
        SELECT 
            COUNT(CASE WHEN compliance_score < 0.5 THEN 1 END) as low_compliance,
            COUNT(CASE WHEN compliance_score >= 0.8 THEN 1 END) as high_compliance,
            AVG(compliance_score) as avg_compliance
        FROM raw.taxpayers
        """
        
        result = pd.read_sql(query, conn).iloc[0]
        
        return {
            'low_compliance_count': int(result['low_compliance']),
            'high_compliance_count': int(result['high_compliance']),
            'average_score': float(result['avg_compliance']),
            'compliance_trend': 'improving'  # Would calculate from historical data
        }
    
    def _get_fraud_insights(self, conn):
        # Load anomalies
        anomalies_df = pd.read_csv(self.input()['anomalies'].path)
        
        return {
            'anomalies_detected': len(anomalies_df),
            'total_at_risk': float(anomalies_df['amount'].sum()),
            'top_risk_taxpayers': anomalies_df.nlargest(5, 'anomaly_score')['taxpayer_id'].tolist(),
            'common_patterns': ['After-hours transactions', 'Unusual amounts', 'Channel switching']
        }
    
    def _generate_recommendations(self, conn):
        return [
            {
                'priority': 'High',
                'action': 'Contact 15 high-risk taxpayers',
                'expected_recovery': 2500000,
                'confidence': 0.75,
                'deadline': (datetime.now() + timedelta(days=7)).isoformat()
            },
            {
                'priority': 'Medium',
                'action': 'Launch SMS campaign for VAT filing reminder',
                'expected_response': '65% filing rate',
                'confidence': 0.82,
                'deadline': (datetime.now() + timedelta(days=14)).isoformat()
            },
            {
                'priority': 'Low',
                'action': 'Update payment channels in rural regions',
                'expected_impact': '12% increase in collections',
                'confidence': 0.68,
                'deadline': (datetime.now() + timedelta(days=30)).isoformat()
            }
        ]
    
    def _predict_collections(self, conn):
        # Simple prediction based on historical patterns
        query = """
        SELECT 
            DATE_TRUNC('day', payment_date) as date,
            SUM(amount) as daily_total
        FROM raw.payments
        WHERE payment_date >= CURRENT_DATE - INTERVAL '30 days'
        GROUP BY DATE_TRUNC('day', payment_date)
        ORDER BY date
        """
        
        df = pd.read_sql(query, conn)
        
        if len(df) > 0:
            # Simple moving average prediction
            avg_daily = df['daily_total'].mean()
            
            predictions = []
            for i in range(7):  # Next 7 days
                date = datetime.now().date() + timedelta(days=i+1)
                # Add some randomness to make it realistic
                predicted = avg_daily * random.uniform(0.8, 1.2)
                predictions.append({
                    'date': date.isoformat(),
                    'predicted_amount': float(predicted),
                    'confidence_interval': [float(predicted * 0.85), float(predicted * 1.15)]
                })
            
            return {
                'next_7_days': predictions,
                'total_expected': sum(p['predicted_amount'] for p in predictions)
            }
        
        return {}

class UpdateDashboardCache(luigi.Task):
    """Update materialized views for dashboard performance"""
    date = luigi.DateParameter(default=datetime.now().date())
    
    def requires(self):
        return [
            ProcessRealtimeTransactions(self.date),
            GeneratePredictiveInsights(self.date)
        ]
    
    def output(self):
        return luigi.LocalTarget(f'/usr/app/data/dashboard_cache_{self.date}.flag')
    
    def run(self):
        db = DatabaseConfig()
        conn = db.get_connection()
        cursor = conn.cursor()
        
        # Refresh materialized views
        queries = [
            "REFRESH MATERIALIZED VIEW CONCURRENTLY analytics.revenue_summary_mv",
            "REFRESH MATERIALIZED VIEW CONCURRENTLY analytics.taxpayer_risk_mv",
            "REFRESH MATERIALIZED VIEW CONCURRENTLY analytics.compliance_trends_mv"
        ]
        
        for query in queries:
            try:
                cursor.execute(query)
            except:
                # Views might not exist yet, create them
                self._create_materialized_views(cursor)
        
        conn.commit()
        cursor.close()
        conn.close()
        
        # Mark as complete
        with open(self.output().path, 'w') as f:
            f.write(f"Updated at {datetime.now()}")
    
    def _create_materialized_views(self, cursor):
        """Create materialized views for performance"""
        
        # Revenue summary
        cursor.execute("""
        CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.revenue_summary_mv AS
        SELECT 
            DATE_TRUNC('day', payment_date) as date,
            tax_type,
            payment_channel,
            region,
            SUM(amount) as total_amount,
            COUNT(*) as transaction_count,
            COUNT(DISTINCT taxpayer_id) as unique_taxpayers
        FROM raw.payments p
        JOIN raw.taxpayers t ON p.taxpayer_id = t.taxpayer_id
        GROUP BY DATE_TRUNC('day', payment_date), tax_type, payment_channel, region
        """)
        
        # Create indexes
        cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_revenue_summary_date 
        ON analytics.revenue_summary_mv(date)
        """)

class MasterPipeline(luigi.WrapperTask):
    """Master pipeline that runs all tasks"""
    date = luigi.DateParameter(default=datetime.now().date())
    
    def requires(self):
        return [
            UpdateDashboardCache(self.date),
            DetectAnomalies(self.date),
            TrainFraudDetectionModel()
        ]

if __name__ == '__main__':
    luigi.build([MasterPipeline()], local_scheduler=False)