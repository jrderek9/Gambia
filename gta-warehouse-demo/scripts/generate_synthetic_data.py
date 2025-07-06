#!/usr/bin/env python3
"""
Synthetic Data Generator for Gambian Tax Authority Demo
Generates realistic tax data with Gambian context
"""

import random
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from faker import Faker
import psycopg2
from psycopg2.extras import execute_batch
import json

fake = Faker()

# Gambian context data
GAMBIAN_NAMES = {
    'first': ['Amadou', 'Fatou', 'Lamin', 'Mariama', 'Ousman', 'Isatou', 'Bakary', 'Kaddy', 
              'Modou', 'Awa', 'Samba', 'Binta', 'Alieu', 'Jainaba', 'Ebrima', 'Fatoumata',
              'Yahya', 'Aminata', 'Demba', 'Sally', 'Musa', 'Haddy', 'Sulayman', 'Kumba'],
    'last': ['Jallow', 'Camara', 'Ceesay', 'Touray', 'Jammeh', 'Sanneh', 'Bojang', 'Darboe',
             'Faye', 'Conteh', 'Jobe', 'Bah', 'Njie', 'Sonko', 'Manneh', 'Barrow', 'Saidy',
             'Colley', 'Cham', 'Drammeh', 'Sanyang', 'Dibba', 'Jatta', 'Sowe']
}

BUSINESS_SECTORS = {
    'Retail': ['General Store', 'Supermarket', 'Electronics', 'Clothing', 'Hardware', 'Pharmacy'],
    'Services': ['Tourism', 'Hospitality', 'Financial Services', 'Telecommunications', 'Transport', 'Real Estate'],
    'Manufacturing': ['Food Processing', 'Textiles', 'Construction Materials', 'Beverages', 'Plastics'],
    'Agriculture': ['Groundnut Processing', 'Rice Milling', 'Fisheries', 'Livestock', 'Horticulture'],
    'Import/Export': ['General Trading', 'Agricultural Export', 'Re-export Trade', 'Import Distribution']
}

REGIONS_DISTRICTS = {
    'Greater Banjul Area': ['Banjul', 'Kanifing', 'Kombo North', 'Kombo South', 'Kombo Central'],
    'West Coast Region': ['Brikama', 'Foni Berefet', 'Foni Bintang', 'Foni Bondali'],
    'Lower River Region': ['Soma', 'Jarra Central', 'Jarra East', 'Kiang Central'],
    'North Bank Region': ['Kerewan', 'Lower Niumi', 'Upper Niumi', 'Jokadu'],
    'Central River Region': ['Janjanbureh', 'Niamina East', 'Niamina West', 'Niani'],
    'Upper River Region': ['Basse', 'Fulladu East', 'Sandu', 'Wuli East']
}

PAYMENT_PROVIDERS = {
    'Mobile Money': ['Africell Money', 'QMoney', 'Afrimoney'],
    'Bank Transfer': ['Trust Bank', 'Access Bank', 'GTBank', 'Ecobank', 'FBN Bank'],
    'Online': ['GTA Portal', 'FinTech Gateway']
}

class GambianTaxDataGenerator:
    def __init__(self, num_taxpayers=50000, start_date='2022-01-01', end_date='2023-12-31'):
        self.num_taxpayers = num_taxpayers
        self.start_date = pd.to_datetime(start_date)
        self.end_date = pd.to_datetime(end_date)
        self.taxpayers = []
        self.fraud_taxpayers = set()
        
    def generate_tin(self):
        """Generate Gambian TIN format: XXX-XXXXXX-X"""
        return f"{random.randint(100, 999)}-{random.randint(100000, 999999)}-{random.randint(1, 9)}"
    
    def generate_gambian_name(self, is_corporate=False):
        """Generate realistic Gambian names"""
        if is_corporate:
            templates = [
                "{} {} Limited", "{} {} Company", "{} Enterprises", "{} Trading",
                "{} & Sons", "{} Holdings", "{} Group", "{} International"
            ]
            template = random.choice(templates)
            if "{}" in template and template.count("{}") > 1:
                return template.format(
                    random.choice(GAMBIAN_NAMES['last']),
                    random.choice(['Brothers', 'Family', 'Associates'])
                )
            else:
                return template.format(random.choice(GAMBIAN_NAMES['last']))
        else:
            return f"{random.choice(GAMBIAN_NAMES['first'])} {random.choice(GAMBIAN_NAMES['last'])}"
    
    def generate_address(self):
        """Generate Gambian addresses"""
        region = random.choice(list(REGIONS_DISTRICTS.keys()))
        district = random.choice(REGIONS_DISTRICTS[region])
        
        if region == 'Greater Banjul Area':
            streets = ['Kairaba Avenue', 'Bertil Harding Highway', 'Independence Drive', 
                      'Atlantic Road', 'Pipeline Road', 'Sait Matty Road']
            return f"{random.randint(1, 200)} {random.choice(streets)}", district, region
        else:
            return f"{district} Town, Plot {random.randint(1, 500)}", district, region
    
    def generate_taxpayers(self):
        """Generate taxpayer records"""
        print("Generating taxpayers...")
        
        for i in range(self.num_taxpayers):
            taxpayer_type = random.choices(
                ['Individual', 'Corporate', 'Partnership', 'NGO'],
                weights=[0.3, 0.5, 0.15, 0.05]
            )[0]
            
            is_corporate = taxpayer_type != 'Individual'
            sector = random.choice(list(BUSINESS_SECTORS.keys())) if is_corporate else 'Services'
            subsector = random.choice(BUSINESS_SECTORS[sector])
            
            address1, district, region = self.generate_address()
            
            # Determine fraud cases (5-10%)
            is_fraud = random.random() < 0.075
            
            taxpayer = {
                'taxpayer_id': f"TP{str(i+1).zfill(6)}",
                'tin': self.generate_tin(),
                'name': self.generate_gambian_name(is_corporate),
                'taxpayer_type': taxpayer_type,
                'registration_date': fake.date_between(start_date='-10y', end_date='-1y'),
                'email': fake.email() if random.random() > 0.3 else None,
                'phone': f"+220{random.randint(2000000, 9999999)}",
                'address_line1': address1,
                'address_line2': None,
                'district': district,
                'region': region,
                'business_sector': sector,
                'business_subsector': subsector,
                'employee_count': random.randint(1, 500) if is_corporate else None,
                'annual_turnover': random.randint(100000, 50000000) if is_corporate else None,
                'risk_category': 'High' if is_fraud else random.choices(['Low', 'Medium', 'High'], weights=[0.7, 0.25, 0.05])[0],
                'compliance_score': random.uniform(0.2, 0.5) if is_fraud else random.uniform(0.6, 0.95)
            }
            
            self.taxpayers.append(taxpayer)
            if is_fraud:
                self.fraud_taxpayers.add(taxpayer['taxpayer_id'])
        
        return pd.DataFrame(self.taxpayers)
    
    def generate_paye_returns(self):
        """Generate PAYE return records"""
        print("Generating PAYE returns...")
        paye_returns = []
        
        corporate_taxpayers = [t for t in self.taxpayers if t['taxpayer_type'] in ['Corporate', 'Partnership']]
        
        for taxpayer in corporate_taxpayers:
            # Generate monthly returns for the period
            current_date = self.start_date
            
            while current_date <= self.end_date:
                # Skip some months for fraud cases
                if taxpayer['taxpayer_id'] in self.fraud_taxpayers and random.random() < 0.3:
                    current_date = current_date + pd.DateOffset(months=1)
                    continue
                
                employee_count = taxpayer['employee_count'] or random.randint(5, 100)
                avg_salary = random.randint(5000, 50000)  # GMD
                
                # Calculate with seasonal variations
                seasonal_factor = 1 + 0.2 * np.sin(current_date.month * np.pi / 6)
                gross_salaries = employee_count * avg_salary * seasonal_factor
                
                # PAYE calculation (simplified Gambian tax rates)
                paye_tax = gross_salaries * random.uniform(0.1, 0.25)
                social_security = gross_salaries * 0.05
                
                # Fraud patterns
                if taxpayer['taxpayer_id'] in self.fraud_taxpayers:
                    # Underreport by 20-50%
                    fraud_factor = random.uniform(0.5, 0.8)
                    paye_tax *= fraud_factor
                    gross_salaries *= fraud_factor
                
                due_date = current_date + pd.DateOffset(days=15)
                filing_date = due_date - timedelta(days=random.randint(-5, 10))
                
                paye_return = {
                    'return_id': f"PAYE{current_date.strftime('%Y%m')}{taxpayer['taxpayer_id'][2:]}",
                    'taxpayer_id': taxpayer['taxpayer_id'],
                    'period_year': current_date.year,
                    'period_month': current_date.month,
                    'filing_date': filing_date if filing_date <= datetime.now() else None,
                    'due_date': due_date,
                    'employee_count': employee_count,
                    'gross_salaries': round(gross_salaries, 2),
                    'paye_tax': round(paye_tax, 2),
                    'social_security': round(social_security, 2),
                    'total_deductions': round(paye_tax + social_security, 2),
                    'net_payment': round(paye_tax, 2),
                    'status': 'Filed' if filing_date and filing_date <= due_date else 'Overdue'
                }
                
                paye_returns.append(paye_return)
                current_date = current_date + pd.DateOffset(months=1)
        
        return pd.DataFrame(paye_returns)
    
    def generate_vat_returns(self):
        """Generate VAT return records"""
        print("Generating VAT returns...")
        vat_returns = []
        
        # VAT registered businesses (turnover > 1M GMD)
        vat_taxpayers = [t for t in self.taxpayers if t.get('annual_turnover', 0) > 1000000]
        
        for taxpayer in vat_taxpayers:
            current_date = self.start_date
            
            while current_date <= self.end_date:
                quarter = (current_date.month - 1) // 3 + 1
                
                # Skip quarters for fraud cases
                if taxpayer['taxpayer_id'] in self.fraud_taxpayers and random.random() < 0.2:
                    current_date = current_date + pd.DateOffset(months=3)
                    continue
                
                # Generate sales based on annual turnover
                quarterly_sales = taxpayer['annual_turnover'] / 4 * random.uniform(0.8, 1.2)
                
                # Seasonal patterns
                if taxpayer['business_sector'] == 'Retail' and quarter == 4:
                    quarterly_sales *= 1.3  # Holiday season boost
                
                taxable_sales = quarterly_sales * 0.7
                exempt_sales = quarterly_sales * 0.2
                export_sales = quarterly_sales * 0.1
                
                output_vat = taxable_sales * 0.15  # 15% VAT rate
                
                # Purchases and input VAT
                purchases = quarterly_sales * random.uniform(0.4, 0.7)
                input_vat = purchases * 0.15 * random.uniform(0.6, 0.9)
                
                # Fraud patterns
                if taxpayer['taxpayer_id'] in self.fraud_taxpayers:
                    # Underreport sales, overreport purchases
                    output_vat *= random.uniform(0.5, 0.7)
                    input_vat *= random.uniform(1.2, 1.5)
                
                net_vat = output_vat - input_vat
                
                due_date = current_date + pd.DateOffset(months=3, days=15)
                filing_date = due_date - timedelta(days=random.randint(-5, 15))
                
                vat_return = {
                    'return_id': f"VAT{current_date.strftime('%Y')}Q{quarter}{taxpayer['taxpayer_id'][2:]}",
                    'taxpayer_id': taxpayer['taxpayer_id'],
                    'period_year': current_date.year,
                    'period_quarter': quarter,
                    'filing_date': filing_date if filing_date <= datetime.now() else None,
                    'due_date': due_date,
                    'total_sales': round(quarterly_sales, 2),
                    'taxable_sales': round(taxable_sales, 2),
                    'exempt_sales': round(exempt_sales, 2),
                    'export_sales': round(export_sales, 2),
                    'output_vat': round(output_vat, 2),
                    'total_purchases': round(purchases, 2),
                    'input_vat': round(input_vat, 2),
                    'net_vat_payable': round(net_vat, 2),
                    'status': 'Filed' if filing_date and filing_date <= due_date else 'Overdue'
                }
                
                vat_returns.append(vat_return)
                current_date = current_date + pd.DateOffset(months=3)
        
        return pd.DataFrame(vat_returns)
    
    def generate_payments(self, paye_df, vat_df):
        """Generate payment records"""
        print("Generating payment records...")
        payments = []
        
        # PAYE payments
        for _, paye in paye_df.iterrows():
            if paye['status'] == 'Filed' and paye['net_payment'] > 0:
                # Most pay on time, some delay
                if paye['taxpayer_id'] in self.fraud_taxpayers:
                    payment_prob = 0.7
                    delay_days = random.randint(0, 30)
                else:
                    payment_prob = 0.95
                    delay_days = random.randint(-5, 5)
                
                if random.random() < payment_prob:
                    payment_date = pd.to_datetime(paye['due_date']) + timedelta(days=delay_days)
                    
                    payment = {
                        'payment_id': f"PAY{payment_date.strftime('%Y%m%d')}{random.randint(1000, 9999)}",
                        'taxpayer_id': paye['taxpayer_id'],
                        'payment_date': payment_date,
                        'payment_channel': random.choices(
                            ['Bank Transfer', 'Mobile Money', 'Online'],
                            weights=[0.5, 0.3, 0.2]
                        )[0],
                        'payment_provider': None,  # Will set based on channel
                        'tax_type': 'PAYE',
                        'period_year': paye['period_year'],
                        'period_month': paye['period_month'],
                        'amount': paye['net_payment'],
                        'reference_number': paye['return_id'],
                        'status': 'Completed'
                    }
                    
                    # Set payment provider
                    payment['payment_provider'] = random.choice(PAYMENT_PROVIDERS[payment['payment_channel']])
                    
                    payments.append(payment)
        
        # VAT payments
        for _, vat in vat_df.iterrows():
            if vat['status'] == 'Filed' and vat['net_vat_payable'] > 0:
                if vat['taxpayer_id'] in self.fraud_taxpayers:
                    payment_prob = 0.6
                    delay_days = random.randint(0, 45)
                else:
                    payment_prob = 0.9
                    delay_days = random.randint(-5, 10)
                
                if random.random() < payment_prob:
                    payment_date = pd.to_datetime(vat['due_date']) + timedelta(days=delay_days)
                    
                    payment = {
                        'payment_id': f"PAY{payment_date.strftime('%Y%m%d')}{random.randint(1000, 9999)}",
                        'taxpayer_id': vat['taxpayer_id'],
                        'payment_date': payment_date,
                        'payment_channel': random.choices(
                            ['Bank Transfer', 'Mobile Money', 'Online'],
                            weights=[0.6, 0.2, 0.2]
                        )[0],
                        'payment_provider': None,
                        'tax_type': 'VAT',
                        'period_year': vat['period_year'],
                        'period_month': vat['period_quarter'] * 3,
                        'amount': vat['net_vat_payable'],
                        'reference_number': vat['return_id'],
                        'status': 'Completed'
                    }
                    
                    payment['payment_provider'] = random.choice(PAYMENT_PROVIDERS[payment['payment_channel']])
                    
                    payments.append(payment)
        
        return pd.DataFrame(payments)
    
    def generate_external_data(self):
        """Generate external registry data"""
        print("Generating external registry data...")
        
        # Companies registry
        companies = []
        corporate_taxpayers = [t for t in self.taxpayers if t['taxpayer_type'] in ['Corporate', 'Partnership']]
        
        for taxpayer in random.sample(corporate_taxpayers, min(len(corporate_taxpayers), 10000)):
            company = {
                'company_reg_no': f"GC{random.randint(10000, 99999)}",
                'company_name': taxpayer['name'],
                'taxpayer_id': taxpayer['taxpayer_id'],
                'incorporation_date': taxpayer['registration_date'] - timedelta(days=random.randint(30, 365)),
                'company_type': taxpayer['taxpayer_type'],
                'share_capital': random.choice([50000, 100000, 250000, 500000, 1000000]),
                'directors_count': random.randint(2, 7),
                'business_activity': taxpayer['business_subsector'],
                'status': 'Active',
                'last_filing_date': fake.date_between(start_date='-1y', end_date='today')
            }
            companies.append(company)
        
        # Vehicle registry
        vehicles = []
        wealthy_taxpayers = random.sample(self.taxpayers, min(len(self.taxpayers), 15000))
        
        for taxpayer in wealthy_taxpayers:
            num_vehicles = random.choices([1, 2, 3, 4], weights=[0.6, 0.3, 0.08, 0.02])[0]
            
            for _ in range(num_vehicles):
                vehicle = {
                    'vehicle_reg_no': f"BJL{random.randint(1000, 9999)}{random.choice(['A', 'B', 'C', 'D'])}",
                    'taxpayer_id': taxpayer['taxpayer_id'],
                    'vehicle_type': random.choice(['Sedan', 'SUV', 'Pickup', 'Van', 'Truck']),
                    'make': random.choice(['Toyota', 'Nissan', 'Mercedes', 'BMW', 'Hyundai', 'Kia']),
                    'model': fake.word().title(),
                    'year': random.randint(2010, 2023),
                    'engine_capacity': random.choice([1300, 1500, 1800, 2000, 2500, 3000]),
                    'purchase_date': fake.date_between(start_date='-5y', end_date='today'),
                    'purchase_value': random.randint(200000, 2000000),
                    'import_duty_paid': None
                }
                vehicle['import_duty_paid'] = vehicle['purchase_value'] * 0.35
                vehicles.append(vehicle)
        
        # Land registry
        properties = []
        property_owners = random.sample(self.taxpayers, min(len(self.taxpayers), 8000))
        
        for taxpayer in property_owners:
            num_properties = random.choices([1, 2, 3], weights=[0.7, 0.25, 0.05])[0]
            
            for _ in range(num_properties):
                property_data = {
                    'property_id': f"LP{random.randint(10000, 99999)}",
                    'taxpayer_id': taxpayer['taxpayer_id'],
                    'property_type': random.choice(['Residential', 'Commercial', 'Industrial', 'Agricultural']),
                    'location': f"{taxpayer['district']}, {taxpayer['region']}",
                    'size_sqm': random.randint(200, 10000),
                    'valuation': random.randint(500000, 10000000),
                    'acquisition_date': fake.date_between(start_date='-10y', end_date='today'),
                    'transfer_tax_paid': None,
                    'annual_property_tax': None
                }
                property_data['transfer_tax_paid'] = property_data['valuation'] * 0.05
                property_data['annual_property_tax'] = property_data['valuation'] * 0.01
                properties.append(property_data)
        
        return pd.DataFrame(companies), pd.DataFrame(vehicles), pd.DataFrame(properties)
    
    def save_to_csv(self, dataframes_dict, output_dir='data'):
        """Save dataframes to CSV files"""
        import os
        os.makedirs(output_dir, exist_ok=True)
        
        for name, df in dataframes_dict.items():
            filepath = os.path.join(output_dir, f"{name}.csv")
            df.to_csv(filepath, index=False)
            print(f"Saved {len(df)} records to {filepath}")
    
    def generate_all_data(self):
        """Generate all datasets"""
        # Generate base data
        taxpayers_df = self.generate_taxpayers()
        paye_df = self.generate_paye_returns()
        vat_df = self.generate_vat_returns()
        payments_df = self.generate_payments(paye_df, vat_df)
        companies_df, vehicles_df, properties_df = self.generate_external_data()
        
        # Create fraud alerts for flagged taxpayers
        fraud_alerts = []
        for taxpayer_id in list(self.fraud_taxpayers)[:100]:  # Top 100 fraud cases
            alert = {
                'taxpayer_id': taxpayer_id,
                'alert_date': fake.date_between(start_date='-6m', end_date='today'),
                'alert_type': random.choice([
                    'Sudden VAT declaration drop',
                    'Inconsistent PAYE vs revenue',
                    'Multiple late payments',
                    'Unusual transaction patterns',
                    'Revenue below industry average'
                ]),
                'risk_score': random.uniform(0.7, 0.95),
                'description': 'Automated detection of suspicious pattern',
                'status': random.choice(['Open', 'Under Investigation', 'Closed']),
                'investigated_by': random.choice(['Sarah Johnson', 'Mohammed Ceesay', 'Fatou Jallow']) if random.random() > 0.5 else None,
                'investigation_notes': None
            }
            fraud_alerts.append(alert)
        
        fraud_alerts_df = pd.DataFrame(fraud_alerts)
        
        # Summary statistics
        print("\n=== Data Generation Summary ===")
        print(f"Taxpayers: {len(taxpayers_df)} ({len(self.fraud_taxpayers)} potential fraud cases)")
        print(f"PAYE Returns: {len(paye_df)}")
        print(f"VAT Returns: {len(vat_df)}")
        print(f"Payments: {len(payments_df)}")
        print(f"Companies: {len(companies_df)}")
        print(f"Vehicles: {len(vehicles_df)}")
        print(f"Properties: {len(properties_df)}")
        print(f"Fraud Alerts: {len(fraud_alerts_df)}")
        
        return {
            'taxpayers': taxpayers_df,
            'paye_returns': paye_df,
            'vat_returns': vat_df,
            'payments': payments_df,
            'companies_registry': companies_df,
            'vehicle_registry': vehicles_df,
            'land_registry': properties_df,
            'fraud_alerts': fraud_alerts_df
        }

if __name__ == "__main__":
    generator = GambianTaxDataGenerator(num_taxpayers=50000)
    all_data = generator.generate_all_data()
    generator.save_to_csv(all_data, output_dir='/opt/airflow/data')