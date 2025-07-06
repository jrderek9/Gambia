-- Quick sample data for testing
-- This creates minimal data to demonstrate the analytics

-- Sample taxpayers
INSERT INTO raw.taxpayers (taxpayer_id, tin, name, taxpayer_type, registration_date, email, phone, district, region, business_sector, business_subsector, annual_turnover, risk_category, compliance_score)
VALUES 
    ('TP000001', '123-456789-0', 'Banjul Trading Co Ltd', 'Corporate', '2020-01-15', 'info@banjultrading.gm', '+2207123456', 'Banjul', 'Greater Banjul Area', 'Retail', 'General Store', 5000000, 'Low', 0.85),
    ('TP000002', '234-567890-1', 'Serrekunda Electronics Ltd', 'Corporate', '2019-03-20', 'contact@serrekunda.gm', '+2207234567', 'Kanifing', 'Greater Banjul Area', 'Retail', 'Electronics', 12500000, 'High', 0.42),
    ('TP000003', '345-678901-2', 'Gambia Fish Export Co', 'Corporate', '2018-06-10', 'export@gambiafish.gm', '+2207345678', 'Bakau', 'Greater Banjul Area', 'Agriculture', 'Fisheries', 25000000, 'Medium', 0.67),
    ('TP000004', '456-789012-3', 'Kairaba Hotel Group', 'Corporate', '2017-11-05', 'info@kairaba.gm', '+2207456789', 'Kololi', 'Greater Banjul Area', 'Services', 'Hospitality', 45000000, 'Low', 0.92),
    ('TP000005', '567-890123-4', 'West Coast Motors', 'Corporate', '2021-02-28', 'sales@westcoast.gm', '+2207567890', 'Brikama', 'West Coast Region', 'Retail', 'Electronics', 8000000, 'Medium', 0.73);

-- Sample PAYE returns
INSERT INTO raw.paye_returns (return_id, taxpayer_id, period_year, period_month, filing_date, due_date, employee_count, gross_salaries, paye_tax, social_security, total_deductions, net_payment, status)
VALUES
    ('PAYE202301TP000001', 'TP000001', 2023, 1, '2023-02-10', '2023-02-15', 25, 500000, 75000, 25000, 100000, 75000, 'Filed'),
    ('PAYE202302TP000001', 'TP000001', 2023, 2, '2023-03-12', '2023-03-15', 25, 520000, 78000, 26000, 104000, 78000, 'Filed'),
    ('PAYE202301TP000002', 'TP000002', 2023, 1, '2023-02-20', '2023-02-15', 45, 900000, 135000, 45000, 180000, 135000, 'Overdue'),
    ('PAYE202302TP000002', 'TP000002', 2023, 2, NULL, '2023-03-15', 45, 850000, 127500, 42500, 170000, 127500, 'Pending');

-- Sample VAT returns
INSERT INTO raw.vat_returns (return_id, taxpayer_id, period_year, period_quarter, filing_date, due_date, total_sales, taxable_sales, exempt_sales, export_sales, output_vat, total_purchases, input_vat, net_vat_payable, status)
VALUES
    ('VAT2023Q1TP000001', 'TP000001', 2023, 1, '2023-04-10', '2023-04-15', 1500000, 1050000, 300000, 150000, 157500, 800000, 120000, 37500, 'Filed'),
    ('VAT2023Q1TP000002', 'TP000002', 2023, 1, '2023-04-20', '2023-04-15', 3000000, 2100000, 600000, 300000, 315000, 1600000, 240000, 75000, 'Overdue'),
    ('VAT2023Q1TP000003', 'TP000003', 2023, 1, '2023-04-05', '2023-04-15', 6000000, 1200000, 600000, 4200000, 180000, 3000000, 150000, 30000, 'Filed');

-- Sample payments
INSERT INTO raw.payments (payment_id, taxpayer_id, payment_date, payment_channel, payment_provider, tax_type, period_year, period_month, amount, reference_number, status)
VALUES
    ('PAY20230215001', 'TP000001', '2023-02-15', 'Bank Transfer', 'Trust Bank', 'PAYE', 2023, 1, 75000, 'PAYE202301TP000001', 'Completed'),
    ('PAY20230315001', 'TP000001', '2023-03-15', 'Mobile Money', 'Africell Money', 'PAYE', 2023, 2, 78000, 'PAYE202302TP000001', 'Completed'),
    ('PAY20230415001', 'TP000001', '2023-04-15', 'Bank Transfer', 'Trust Bank', 'VAT', 2023, 3, 37500, 'VAT2023Q1TP000001', 'Completed'),
    ('PAY20230225001', 'TP000002', '2023-02-25', 'Bank Transfer', 'GTBank', 'PAYE', 2023, 1, 135000, 'PAYE202301TP000002', 'Completed');

-- Sample fraud alerts
INSERT INTO analytics.fraud_alerts (taxpayer_id, alert_date, alert_type, risk_score, description, status)
VALUES
    ('TP000002', '2023-05-01', 'Sudden VAT Declaration Drop', 0.87, 'VAT sales dropped by 45% in Q1 2023', 'Open'),
    ('TP000002', '2023-05-01', 'Multiple Late Payments', 0.75, 'Consistent pattern of late PAYE payments', 'Open');