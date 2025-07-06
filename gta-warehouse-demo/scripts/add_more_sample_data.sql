-- Add more sample data for a richer demo

-- Add more recent payments to show current activity
INSERT INTO raw.payments (payment_id, taxpayer_id, payment_date, payment_channel, payment_provider, tax_type, period_year, period_month, amount, reference_number, status)
VALUES
    -- July 2025 payments (current month)
    ('PAY20250701001', 'TP000001', '2025-07-01', 'Bank Transfer', 'Trust Bank', 'PAYE', 2025, 6, 82000, 'PAYE202506TP000001', 'Completed'),
    ('PAY20250702001', 'TP000003', '2025-07-02', 'Mobile Money', 'QMoney', 'VAT', 2025, 6, 45000, 'VAT2025Q2TP000003', 'Completed'),
    ('PAY20250703001', 'TP000004', '2025-07-03', 'Online', 'GTA Portal', 'Corporate Tax', 2025, 12, 1250000, 'CORP2025TP000004', 'Completed'),
    ('PAY20250704001', 'TP000005', '2025-07-04', 'Mobile Money', 'Africell Money', 'PAYE', 2025, 6, 125000, 'PAYE202506TP000005', 'Completed'),
    
    -- June 2025 payments
    ('PAY20250615001', 'TP000001', '2025-06-15', 'Bank Transfer', 'Trust Bank', 'PAYE', 2025, 5, 80000, 'PAYE202505TP000001', 'Completed'),
    ('PAY20250615002', 'TP000002', '2025-06-15', 'Bank Transfer', 'GTBank', 'PAYE', 2025, 5, 142000, 'PAYE202505TP000002', 'Completed'),
    ('PAY20250620001', 'TP000003', '2025-06-20', 'Mobile Money', 'QMoney', 'VAT', 2025, 3, 42000, 'VAT2025Q1TP000003', 'Completed'),
    ('PAY20250625001', 'TP000004', '2025-06-25', 'Online', 'GTA Portal', 'VAT', 2025, 3, 185000, 'VAT2025Q1TP000004', 'Completed'),
    
    -- May 2025 payments  
    ('PAY20250515001', 'TP000001', '2025-05-15', 'Mobile Money', 'Africell Money', 'PAYE', 2025, 4, 78000, 'PAYE202504TP000001', 'Completed'),
    ('PAY20250515002', 'TP000002', '2025-05-15', 'Bank Transfer', 'GTBank', 'PAYE', 2025, 4, 138000, 'PAYE202504TP000002', 'Completed'),
    ('PAY20250515003', 'TP000005', '2025-05-15', 'Bank Transfer', 'Access Bank', 'PAYE', 2025, 4, 120000, 'PAYE202504TP000005', 'Completed');

-- Add more taxpayers from different regions
INSERT INTO raw.taxpayers (taxpayer_id, tin, name, taxpayer_type, registration_date, email, phone, district, region, business_sector, business_subsector, annual_turnover, risk_category, compliance_score)
VALUES 
    ('TP000006', '678-901234-5', 'Brikama Construction Ltd', 'Corporate', '2020-08-10', 'info@brikamacon.gm', '+2208123456', 'Brikama', 'West Coast Region', 'Manufacturing', 'Construction Materials', 15000000, 'Low', 0.88),
    ('TP000007', '789-012345-6', 'Basse Agricultural Export', 'Corporate', '2019-05-15', 'export@basseagri.gm', '+2209234567', 'Basse', 'Upper River Region', 'Agriculture', 'Agricultural Export', 22000000, 'Medium', 0.71),
    ('TP000008', '890-123456-7', 'Janjanbureh Tourism Co', 'Corporate', '2021-03-20', 'info@janjantours.gm', '+2206345678', 'Janjanbureh', 'Central River Region', 'Services', 'Tourism', 8500000, 'Low', 0.90),
    ('TP000009', '901-234567-8', 'Kerewan Fishing Coop', 'Partnership', '2018-11-25', 'coop@kerewanfish.gm', '+2207456789', 'Kerewan', 'North Bank Region', 'Agriculture', 'Fisheries', 12000000, 'Medium', 0.76),
    ('TP000010', '012-345678-9', 'Soma Transport Services', 'Corporate', '2020-02-14', 'transport@soma.gm', '+2205567890', 'Soma', 'Lower River Region', 'Services', 'Transport', 18000000, 'Low', 0.82);

-- Add payments for new taxpayers
INSERT INTO raw.payments (payment_id, taxpayer_id, payment_date, payment_channel, payment_provider, tax_type, period_year, period_month, amount, reference_number, status)
VALUES
    ('PAY20250705001', 'TP000006', '2025-07-05', 'Bank Transfer', 'Ecobank', 'VAT', 2025, 6, 95000, 'VAT2025Q2TP000006', 'Completed'),
    ('PAY20250705002', 'TP000007', '2025-07-05', 'Mobile Money', 'QMoney', 'Corporate Tax', 2025, 12, 850000, 'CORP2025TP000007', 'Completed'),
    ('PAY20250706001', 'TP000008', '2025-07-06', 'Online', 'GTA Portal', 'PAYE', 2025, 6, 65000, 'PAYE202506TP000008', 'Completed'),
    ('PAY20250706002', 'TP000009', '2025-07-06', 'Bank Transfer', 'FBN Bank', 'VAT', 2025, 6, 78000, 'VAT2025Q2TP000009', 'Completed'),
    ('PAY20250706003', 'TP000010', '2025-07-06', 'Mobile Money', 'Afrimoney', 'PAYE', 2025, 6, 110000, 'PAYE202506TP000010', 'Completed');

-- Update fraud alerts with more recent dates
UPDATE analytics.fraud_alerts 
SET alert_date = '2025-07-01' 
WHERE alert_date < '2025-01-01';

-- Add more fraud alerts
INSERT INTO analytics.fraud_alerts (taxpayer_id, alert_date, alert_type, risk_score, description, status)
VALUES
    ('TP000005', '2025-07-03', 'Below Peer Tax Contribution', 0.65, 'Paying 55% less than similar electronics retailers', 'Open'),
    ('TP000007', '2025-07-04', 'Payment Channel Switching', 0.72, 'Frequent changes between payment methods detected', 'Open'),
    ('TP000009', '2025-07-05', 'Inconsistent PAYE vs Revenue', 0.68, 'Employee count doesn''t match declared revenue levels', 'Open');