#!/usr/bin/env python3
"""
Enhanced Data Generator for GTA Demo
Creates comprehensive, realistic data with patterns for impressive demos
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
from faker import Faker
import psycopg2
from psycopg2.extras import execute_batch

fake = Faker()

# Gambian context
REGIONS = {
    'Greater Banjul Area': {
        'districts': ['Banjul', 'Kanifing', 'Kombo North', 'Kombo South'],
        'business_concentration': 0.45,
        'digital_adoption': 0.7
    },
    'West Coast Region': {
        'districts': ['Brikama', 'Foni'],
        'business_concentration': 0.2,
        'digital_adoption': 0.5
    },
    'Lower River Region': {
        'districts': ['Soma', 'Jarra'],
        'business_concentration': 0.1,
        'digital_adoption': 0.3
    },
    'North Bank Region': {
        'districts': ['Kerewan', 'Niumi'],
        'business_concentration': 0.1,
        'digital_adoption': 0.25
    },
    'Central River Region': {
        'districts': ['Janjanbureh', 'Niani'],
        'business_concentration': 0.08,
        'digital_adoption': 0.2
    },
    'Upper River Region': {
        'districts': ['Basse', 'Wuli'],
        'business_concentration': 0.07,
        'digital_adoption': 0.15
    }
}

BUSINESS_PROFILES = {
    'Telecommunications': {
        'avg_revenue': 50000000,
        'compliance_rate': 0.85,
        'digital_payment': 0.9,
        'growth_rate': 0.15
    },
    'Banking': {
        'avg_revenue': 100000000,
        'compliance_rate': 0.95,
        'digital_payment': 0.95,
        'growth_rate': 0.12
    },
    'Retail': {
        'avg_revenue': 5000000,
        'compliance_rate': 0.65,
        'digital_payment': 0.4,
        'growth_rate': 0.08
    },
    'Manufacturing': {
        'avg_revenue': 20000000,
        'compliance_rate': 0.75,
        'digital_payment': 0.6,
        'growth_rate': 0.1
    },
    'Tourism': {
        'avg_revenue': 15000000,
        'compliance_rate': 0.7,
        'digital_payment': 0.7,
        'growth_rate': -0.05  # COVID impact
    },
    'Agriculture': {
        'avg_revenue': 8000000,
        'compliance_rate': 0.55,
        'digital_payment': 0.2,
        'growth_rate': 0.06
    }
}

class ComprehensiveDataGenerator:
    def __init__(self, num_taxpayers=10000):
        self.num_taxpayers = num_taxpayers
        self.start_date = datetime.now() - timedelta(days=730)  # 2 years
        self.end_date = datetime.now()
        
    def connect_db(self):
        return psycopg2.connect(
            host="localhost",
            port=5432,
            database="gta_warehouse",
            user="gta_admin",
            password="gta_secure_pass"
        )
    
    def generate_taxpayers(self):
        """Generate diverse taxpayer base"""
        taxpayers = []
        
        # Distribute taxpayers by region based on business concentration
        for i in range(self.num_taxpayers):
            # Select region based on business concentration
            regions = list(REGIONS.keys())
            weights = [REGIONS[r]['business_concentration'] for r in regions]
            region = np.random.choice(regions, p=weights)
            
            # Select business type
            if region == 'Greater Banjul Area':
                # More diverse businesses in capital
                business_types = list(BUSINESS_PROFILES.keys())
                business_weights = [0.15, 0.2, 0.3, 0.15, 0.1, 0.1]
            else:
                # More agriculture and retail in other regions
                business_types = list(BUSINESS_PROFILES.keys())
                business_weights = [0.05, 0.05, 0.35, 0.1, 0.05, 0.4]
            
            business_type = np.random.choice(business_types, p=business_weights)
            profile = BUSINESS_PROFILES[business_type]
            
            # Generate realistic revenue based on business type
            revenue = np.random.lognormal(
                np.log(profile['avg_revenue']), 
                0.5
            )
            
            # Determine risk based on various factors
            risk_factors = []
            
            # Low compliance sectors
            if profile['compliance_rate'] < 0.6:
                risk_factors.append(0.2)
            
            # New businesses
            years_active = random.randint(0, 20)
            if years_active < 2:
                risk_factors.append(0.15)
            
            # Revenue inconsistencies
            if random.random() < 0.1:  # 10% have inconsistencies
                risk_factors.append(0.3)
            
            # Cash-heavy businesses
            if profile['digital_payment'] < 0.3:
                risk_factors.append(0.1)
            
            risk_score = min(0.95, sum(risk_factors))
            
            # Determine compliance score inversely related to risk
            compliance_score = max(0.1, min(0.95, 
                np.random.normal(profile['compliance_rate'], 0.15) - risk_score/2
            ))
            
            taxpayer = {
                'taxpayer_id': f'TP{str(i+1).zfill(6)}',
                'tin': f'{random.randint(100,999)}-{random.randint(100000,999999)}-{random.randint(1,9)}',
                'name': self._generate_business_name(business_type, region),
                'taxpayer_type': np.random.choice(['Corporate', 'Partnership', 'Individual'], p=[0.7, 0.2, 0.1]),
                'registration_date': self.start_date + timedelta(days=random.randint(0, 365*years_active)),
                'email': f'contact{i}@{fake.domain_name()}',
                'phone': f'+220{random.randint(2000000, 9999999)}',
                'address_line1': f'{random.randint(1,500)} {self._get_street_name(region)}',
                'district': random.choice(REGIONS[region]['districts']),
                'region': region,
                'business_sector': business_type,
                'business_subsector': self._get_subsector(business_type),
                'employee_count': int(np.random.lognormal(np.log(20), 1.5)),
                'annual_turnover': revenue,
                'risk_category': self._categorize_risk(risk_score),
                'compliance_score': compliance_score
            }
            
            taxpayers.append(taxpayer)
        
        return pd.DataFrame(taxpayers)
    
    def generate_payment_patterns(self, taxpayers_df):
        """Generate realistic payment patterns"""
        payments = []
        
        for _, taxpayer in taxpayers_df.iterrows():
            # Generate payments based on compliance score
            if taxpayer['compliance_score'] > 0.8:
                # Good taxpayers pay regularly
                payment_frequency = 'monthly'
                payment_probability = 0.95
            elif taxpayer['compliance_score'] > 0.5:
                # Average taxpayers
                payment_frequency = 'quarterly'
                payment_probability = 0.7
            else:
                # Poor compliance
                payment_frequency = 'sporadic'
                payment_probability = 0.4
            
            # Generate historical payments
            current_date = self.start_date
            
            while current_date <= self.end_date:
                if random.random() < payment_probability:
                    # Determine payment amount based on business profile
                    profile = BUSINESS_PROFILES.get(taxpayer['business_sector'], BUSINESS_PROFILES['Retail'])
                    
                    # PAYE payments (monthly)
                    if taxpayer['employee_count'] > 0:
                        paye_amount = taxpayer['employee_count'] * random.uniform(3000, 8000) * 0.15
                        
                        # Add seasonal variations
                        if current_date.month in [11, 12]:  # Bonus season
                            paye_amount *= 1.3
                        
                        payment = {
                            'payment_id': f'PAY{current_date.strftime("%Y%m%d")}{random.randint(10000, 99999)}',
                            'taxpayer_id': taxpayer['taxpayer_id'],
                            'payment_date': current_date + timedelta(days=random.randint(0, 15)),
                            'payment_channel': self._get_payment_channel(taxpayer['region'], profile['digital_payment']),
                            'payment_provider': self._get_payment_provider(),
                            'tax_type': 'PAYE',
                            'period_year': current_date.year,
                            'period_month': current_date.month,
                            'amount': round(paye_amount, 2),
                            'reference_number': f'PAYE{current_date.strftime("%Y%m")}{taxpayer["taxpayer_id"][2:]}',
                            'status': 'Completed'
                        }
                        payments.append(payment)
                    
                    # VAT payments (quarterly)
                    if current_date.month % 3 == 0:
                        vat_base = taxpayer['annual_turnover'] / 4
                        # Add business cycle variations
                        vat_amount = vat_base * random.uniform(0.8, 1.2) * 0.15
                        
                        payment = {
                            'payment_id': f'PAY{current_date.strftime("%Y%m%d")}{random.randint(10000, 99999)}',
                            'taxpayer_id': taxpayer['taxpayer_id'],
                            'payment_date': current_date + timedelta(days=random.randint(0, 20)),
                            'payment_channel': self._get_payment_channel(taxpayer['region'], profile['digital_payment']),
                            'payment_provider': self._get_payment_provider(),
                            'tax_type': 'VAT',
                            'period_year': current_date.year,
                            'period_month': current_date.month,
                            'amount': round(vat_amount, 2),
                            'reference_number': f'VAT{current_date.strftime("%Y")}Q{(current_date.month-1)//3+1}{taxpayer["taxpayer_id"][2:]}',
                            'status': 'Completed'
                        }
                        payments.append(payment)
                
                # Move to next period
                if payment_frequency == 'monthly':
                    current_date += timedelta(days=30)
                elif payment_frequency == 'quarterly':
                    current_date += timedelta(days=90)
                else:
                    current_date += timedelta(days=random.randint(30, 120))
        
        return pd.DataFrame(payments)
    
    def generate_fraud_patterns(self, taxpayers_df, payments_df):
        """Generate sophisticated fraud patterns"""
        fraud_alerts = []
        
        # Pattern 1: Sudden revenue drops
        revenue_by_taxpayer = payments_df.groupby(['taxpayer_id', pd.Grouper(key='payment_date', freq='Q')])['amount'].sum().reset_index()
        
        for taxpayer_id in revenue_by_taxpayer['taxpayer_id'].unique():
            taxpayer_revenue = revenue_by_taxpayer[revenue_by_taxpayer['taxpayer_id'] == taxpayer_id].sort_values('payment_date')
            
            if len(taxpayer_revenue) > 4:
                # Calculate rolling average
                taxpayer_revenue['rolling_avg'] = taxpayer_revenue['amount'].rolling(4).mean()
                taxpayer_revenue['pct_change'] = (taxpayer_revenue['amount'] - taxpayer_revenue['rolling_avg']) / taxpayer_revenue['rolling_avg']
                
                # Flag significant drops
                suspicious = taxpayer_revenue[taxpayer_revenue['pct_change'] < -0.3]
                
                for _, row in suspicious.iterrows():
                    alert = {
                        'taxpayer_id': taxpayer_id,
                        'alert_date': row['payment_date'] + timedelta(days=30),
                        'alert_type': 'Sudden Revenue Drop',
                        'risk_score': min(0.95, abs(row['pct_change'])),
                        'description': f'Revenue dropped by {abs(row["pct_change"])*100:.1f}% compared to 4-quarter average',
                        'status': 'Open'
                    }
                    fraud_alerts.append(alert)
        
        # Pattern 2: Payment channel anomalies
        channel_patterns = payments_df.groupby(['taxpayer_id', 'payment_channel']).size().reset_index(name='count')
        
        for taxpayer_id in channel_patterns['taxpayer_id'].unique():
            channels = channel_patterns[channel_patterns['taxpayer_id'] == taxpayer_id]
            
            if len(channels) > 3:  # Using too many channels
                alert = {
                    'taxpayer_id': taxpayer_id,
                    'alert_date': datetime.now().date(),
                    'alert_type': 'Payment Channel Anomaly',
                    'risk_score': min(0.8, len(channels) * 0.2),
                    'description': f'Using {len(channels)} different payment channels - possible structuring',
                    'status': 'Open'
                }
                fraud_alerts.append(alert)
        
        # Pattern 3: Industry comparison
        industry_avg = payments_df.merge(taxpayers_df[['taxpayer_id', 'business_sector']], on='taxpayer_id')
        industry_avg = industry_avg.groupby(['business_sector', 'tax_type'])['amount'].mean().reset_index()
        
        taxpayer_avg = payments_df.groupby(['taxpayer_id', 'tax_type'])['amount'].mean().reset_index()
        taxpayer_avg = taxpayer_avg.merge(taxpayers_df[['taxpayer_id', 'business_sector']], on='taxpayer_id')
        taxpayer_avg = taxpayer_avg.merge(industry_avg, on=['business_sector', 'tax_type'], suffixes=('', '_industry'))
        
        taxpayer_avg['variance'] = (taxpayer_avg['amount'] - taxpayer_avg['amount_industry']) / taxpayer_avg['amount_industry']
        
        # Flag those paying significantly less than industry average
        suspicious = taxpayer_avg[taxpayer_avg['variance'] < -0.5]
        
        for _, row in suspicious.iterrows():
            alert = {
                'taxpayer_id': row['taxpayer_id'],
                'alert_date': datetime.now().date(),
                'alert_type': 'Below Industry Average',
                'risk_score': min(0.85, abs(row['variance'])),
                'description': f'{row["tax_type"]} payments {abs(row["variance"])*100:.1f}% below industry average',
                'status': 'Open'
            }
            fraud_alerts.append(alert)
        
        return pd.DataFrame(fraud_alerts)
    
    def _generate_business_name(self, business_type, region):
        """Generate realistic Gambian business names"""
        prefixes = {
            'Telecommunications': ['Gamtel', 'Africell', 'QCell', 'Comium'],
            'Banking': ['Trust Bank', 'GTBank', 'Access Bank', 'Ecobank', 'FBN'],
            'Retail': ['Kairaba', 'Senegambia', 'Albert Market', 'Westfield'],
            'Manufacturing': ['Gambia', 'Banjul', 'Kombo', 'National'],
            'Tourism': ['Paradise', 'Sunset', 'Atlantic', 'Kololi', 'Kotu'],
            'Agriculture': ['Gambia', 'River', 'Farm', 'Agro', 'Green']
        }
        
        suffixes = {
            'Telecommunications': ['Communications', 'Telecom', 'Mobile', 'Networks'],
            'Banking': ['Bank', 'Finance', 'Capital', 'Microfinance'],
            'Retail': ['Supermarket', 'Store', 'Trading', 'Enterprises', 'Mart'],
            'Manufacturing': ['Industries', 'Factory', 'Processing', 'Manufacturing'],
            'Tourism': ['Hotel', 'Resort', 'Lodge', 'Tours', 'Beach Resort'],
            'Agriculture': ['Farms', 'Agro', 'Cooperative', 'Produce', 'Export']
        }
        
        prefix = random.choice(prefixes.get(business_type, ['Gambia']))
        suffix = random.choice(suffixes.get(business_type, ['Limited']))
        
        return f'{prefix} {suffix}'
    
    def _get_street_name(self, region):
        """Get region-appropriate street names"""
        streets = {
            'Greater Banjul Area': ['Kairaba Avenue', 'Independence Drive', 'Atlantic Road', 
                                   'Bertil Harding Highway', 'Pipeline Road'],
            'default': ['Main Street', 'Market Road', 'Highway', 'Town Center']
        }
        
        return random.choice(streets.get(region, streets['default']))
    
    def _get_subsector(self, sector):
        """Get business subsector"""
        subsectors = {
            'Telecommunications': ['Mobile Services', 'Internet Services', 'Data Centers'],
            'Banking': ['Commercial Banking', 'Microfinance', 'Investment Banking'],
            'Retail': ['Supermarket', 'Electronics', 'Clothing', 'Hardware'],
            'Manufacturing': ['Food Processing', 'Textiles', 'Construction Materials'],
            'Tourism': ['Hotels', 'Restaurants', 'Tour Operations', 'Travel Agency'],
            'Agriculture': ['Crop Production', 'Livestock', 'Fisheries', 'Export']
        }
        
        return random.choice(subsectors.get(sector, ['General']))
    
    def _categorize_risk(self, risk_score):
        """Categorize risk level"""
        if risk_score > 0.7:
            return 'High'
        elif risk_score > 0.4:
            return 'Medium'
        else:
            return 'Low'
    
    def _get_payment_channel(self, region, digital_adoption):
        """Determine payment channel based on region and digital adoption"""
        if random.random() < REGIONS[region]['digital_adoption'] * digital_adoption:
            return np.random.choice(['Online', 'Mobile Money', 'Bank Transfer'], p=[0.2, 0.4, 0.4])
        else:
            return np.random.choice(['Bank Transfer', 'Cash', 'Cheque'], p=[0.5, 0.3, 0.2])
    
    def _get_payment_provider(self):
        """Get payment provider"""
        providers = {
            'Mobile Money': ['Africell Money', 'QMoney', 'Afrimoney'],
            'Bank Transfer': ['Trust Bank', 'GTBank', 'Access Bank', 'Ecobank'],
            'Online': ['GTA Portal', 'PayGov', 'FinTech Gateway']
        }
        
        return random.choice(providers.get('Mobile Money', ['Direct']))
    
    def load_to_database(self, dataframes):
        """Load data to PostgreSQL"""
        conn = self.connect_db()
        cursor = conn.cursor()
        
        try:
            # Clear existing data
            tables = ['raw.payments', 'analytics.fraud_alerts', 'raw.taxpayers']
            for table in tables:
                cursor.execute(f"TRUNCATE TABLE {table} CASCADE")
            
            # Load taxpayers
            print("Loading taxpayers...")
            taxpayers_data = [tuple(row) for row in dataframes['taxpayers'].to_records(index=False)]
            execute_batch(cursor, """
                INSERT INTO raw.taxpayers (
                    taxpayer_id, tin, name, taxpayer_type, registration_date,
                    email, phone, address_line1, address_line2, district, region,
                    business_sector, business_subsector, employee_count,
                    annual_turnover, risk_category, compliance_score
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, taxpayers_data)
            
            # Load payments
            print("Loading payments...")
            payments_data = [tuple(row) for row in dataframes['payments'].to_records(index=False)]
            execute_batch(cursor, """
                INSERT INTO raw.payments (
                    payment_id, taxpayer_id, payment_date, payment_channel,
                    payment_provider, tax_type, period_year, period_month,
                    amount, reference_number, status
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, payments_data)
            
            # Load fraud alerts
            print("Loading fraud alerts...")
            fraud_data = [tuple(row) for row in dataframes['fraud_alerts'].to_records(index=False)]
            execute_batch(cursor, """
                INSERT INTO analytics.fraud_alerts (
                    taxpayer_id, alert_date, alert_type, risk_score,
                    description, status
                ) VALUES (%s, %s, %s, %s, %s, %s)
            """, fraud_data)
            
            conn.commit()
            print("Data loaded successfully!")
            
        except Exception as e:
            conn.rollback()
            print(f"Error loading data: {e}")
            raise
        finally:
            cursor.close()
            conn.close()
    
    def generate_all(self):
        """Generate all data"""
        print("Generating comprehensive demo data...")
        
        # Generate base data
        print("- Generating taxpayers...")
        taxpayers_df = self.generate_taxpayers()
        
        print("- Generating payment patterns...")
        payments_df = self.generate_payment_patterns(taxpayers_df)
        
        print("- Generating fraud patterns...")
        fraud_df = self.generate_fraud_patterns(taxpayers_df, payments_df)
        
        # Summary statistics
        print("\n=== Generation Summary ===")
        print(f"Taxpayers: {len(taxpayers_df):,}")
        print(f"Payments: {len(payments_df):,}")
        print(f"Fraud Alerts: {len(fraud_df):,}")
        print(f"Total Revenue: D {payments_df['amount'].sum():,.2f}")
        
        # Regional distribution
        print("\nRegional Distribution:")
        for region, count in taxpayers_df['region'].value_counts().items():
            print(f"  {region}: {count:,} ({count/len(taxpayers_df)*100:.1f}%)")
        
        return {
            'taxpayers': taxpayers_df,
            'payments': payments_df,
            'fraud_alerts': fraud_df
        }

if __name__ == "__main__":
    generator = ComprehensiveDataGenerator(num_taxpayers=10000)
    data = generator.generate_all()
    
    # Save to CSV for backup
    for name, df in data.items():
        df.to_csv(f'/usr/app/data/{name}_comprehensive.csv', index=False)
    
    # Load to database
    generator.load_to_database(data)