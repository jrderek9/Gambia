-- GTA Data Warehouse Schema
-- Initialize database with tables for Gambian Tax Authority

-- Create schemas
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS analytics;

-- Taxpayers table
CREATE TABLE IF NOT EXISTS raw.taxpayers (
    taxpayer_id VARCHAR(20) PRIMARY KEY,
    tin VARCHAR(15) UNIQUE NOT NULL,
    name VARCHAR(200) NOT NULL,
    taxpayer_type VARCHAR(50) CHECK (taxpayer_type IN ('Individual', 'Corporate', 'Partnership', 'NGO')),
    registration_date DATE NOT NULL,
    email VARCHAR(150),
    phone VARCHAR(20),
    address_line1 VARCHAR(200),
    address_line2 VARCHAR(200),
    district VARCHAR(100),
    region VARCHAR(100),
    business_sector VARCHAR(100),
    business_subsector VARCHAR(100),
    employee_count INTEGER,
    annual_turnover DECIMAL(15, 2),
    risk_category VARCHAR(20) DEFAULT 'Low',
    compliance_score DECIMAL(3, 2) DEFAULT 0.75,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- PAYE (Pay As You Earn) returns
CREATE TABLE IF NOT EXISTS raw.paye_returns (
    return_id VARCHAR(20) PRIMARY KEY,
    taxpayer_id VARCHAR(20) REFERENCES raw.taxpayers(taxpayer_id),
    period_year INTEGER NOT NULL,
    period_month INTEGER NOT NULL,
    filing_date DATE,
    due_date DATE NOT NULL,
    employee_count INTEGER,
    gross_salaries DECIMAL(15, 2),
    paye_tax DECIMAL(15, 2),
    social_security DECIMAL(15, 2),
    total_deductions DECIMAL(15, 2),
    net_payment DECIMAL(15, 2),
    status VARCHAR(20) CHECK (status IN ('Filed', 'Pending', 'Overdue', 'Amended')),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- VAT returns
CREATE TABLE IF NOT EXISTS raw.vat_returns (
    return_id VARCHAR(20) PRIMARY KEY,
    taxpayer_id VARCHAR(20) REFERENCES raw.taxpayers(taxpayer_id),
    period_year INTEGER NOT NULL,
    period_quarter INTEGER NOT NULL,
    filing_date DATE,
    due_date DATE NOT NULL,
    total_sales DECIMAL(15, 2),
    taxable_sales DECIMAL(15, 2),
    exempt_sales DECIMAL(15, 2),
    export_sales DECIMAL(15, 2),
    output_vat DECIMAL(15, 2),
    total_purchases DECIMAL(15, 2),
    input_vat DECIMAL(15, 2),
    net_vat_payable DECIMAL(15, 2),
    status VARCHAR(20) CHECK (status IN ('Filed', 'Pending', 'Overdue', 'Amended')),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Corporate tax assessments
CREATE TABLE IF NOT EXISTS raw.corporate_tax (
    assessment_id VARCHAR(20) PRIMARY KEY,
    taxpayer_id VARCHAR(20) REFERENCES raw.taxpayers(taxpayer_id),
    tax_year INTEGER NOT NULL,
    filing_date DATE,
    due_date DATE NOT NULL,
    gross_income DECIMAL(15, 2),
    deductible_expenses DECIMAL(15, 2),
    taxable_income DECIMAL(15, 2),
    tax_rate DECIMAL(5, 2),
    tax_liability DECIMAL(15, 2),
    tax_credits DECIMAL(15, 2),
    net_tax_payable DECIMAL(15, 2),
    status VARCHAR(20) CHECK (status IN ('Filed', 'Pending', 'Overdue', 'Under Review', 'Amended')),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Payment transactions
CREATE TABLE IF NOT EXISTS raw.payments (
    payment_id VARCHAR(20) PRIMARY KEY,
    taxpayer_id VARCHAR(20) REFERENCES raw.taxpayers(taxpayer_id),
    payment_date DATE NOT NULL,
    payment_channel VARCHAR(50) CHECK (payment_channel IN ('Bank Transfer', 'Mobile Money', 'Cash', 'Cheque', 'POS', 'Online')),
    payment_provider VARCHAR(100),
    tax_type VARCHAR(50) CHECK (tax_type IN ('PAYE', 'VAT', 'Corporate Tax', 'Withholding Tax', 'Capital Gains', 'Property Tax')),
    period_year INTEGER,
    period_month INTEGER,
    amount DECIMAL(15, 2) NOT NULL,
    reference_number VARCHAR(50),
    status VARCHAR(20) CHECK (status IN ('Completed', 'Pending', 'Failed', 'Reversed')),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- External data: Companies registry
CREATE TABLE IF NOT EXISTS raw.companies_registry (
    company_reg_no VARCHAR(20) PRIMARY KEY,
    company_name VARCHAR(200) NOT NULL,
    taxpayer_id VARCHAR(20),
    incorporation_date DATE,
    company_type VARCHAR(50),
    share_capital DECIMAL(15, 2),
    directors_count INTEGER,
    business_activity VARCHAR(500),
    status VARCHAR(20),
    last_filing_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- External data: Vehicle registry
CREATE TABLE IF NOT EXISTS raw.vehicle_registry (
    vehicle_reg_no VARCHAR(20) PRIMARY KEY,
    taxpayer_id VARCHAR(20),
    vehicle_type VARCHAR(50),
    make VARCHAR(50),
    model VARCHAR(50),
    year INTEGER,
    engine_capacity INTEGER,
    purchase_date DATE,
    purchase_value DECIMAL(15, 2),
    import_duty_paid DECIMAL(15, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- External data: Land registry
CREATE TABLE IF NOT EXISTS raw.land_registry (
    property_id VARCHAR(20) PRIMARY KEY,
    taxpayer_id VARCHAR(20),
    property_type VARCHAR(50),
    location VARCHAR(200),
    size_sqm DECIMAL(10, 2),
    valuation DECIMAL(15, 2),
    acquisition_date DATE,
    transfer_tax_paid DECIMAL(15, 2),
    annual_property_tax DECIMAL(15, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Fraud detection alerts
CREATE TABLE IF NOT EXISTS analytics.fraud_alerts (
    alert_id SERIAL PRIMARY KEY,
    taxpayer_id VARCHAR(20) REFERENCES raw.taxpayers(taxpayer_id),
    alert_date DATE NOT NULL,
    alert_type VARCHAR(100),
    risk_score DECIMAL(3, 2),
    description TEXT,
    status VARCHAR(20) DEFAULT 'Open',
    investigated_by VARCHAR(100),
    investigation_notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for performance
CREATE INDEX idx_taxpayers_tin ON raw.taxpayers(tin);
CREATE INDEX idx_taxpayers_region ON raw.taxpayers(region);
CREATE INDEX idx_taxpayers_sector ON raw.taxpayers(business_sector);
CREATE INDEX idx_paye_taxpayer_period ON raw.paye_returns(taxpayer_id, period_year, period_month);
CREATE INDEX idx_vat_taxpayer_period ON raw.vat_returns(taxpayer_id, period_year, period_quarter);
CREATE INDEX idx_corporate_taxpayer_year ON raw.corporate_tax(taxpayer_id, tax_year);
CREATE INDEX idx_payments_taxpayer_date ON raw.payments(taxpayer_id, payment_date);
CREATE INDEX idx_payments_type_date ON raw.payments(tax_type, payment_date);

-- Gambian regions for reference
CREATE TABLE IF NOT EXISTS raw.regions (
    region_id SERIAL PRIMARY KEY,
    region_name VARCHAR(100) UNIQUE NOT NULL,
    districts TEXT[]
);

INSERT INTO raw.regions (region_name, districts) VALUES
('Greater Banjul Area', ARRAY['Banjul', 'Kanifing', 'Kombo North', 'Kombo South', 'Kombo Central', 'Kombo East']),
('West Coast Region', ARRAY['Foni Berefet', 'Foni Bintang', 'Foni Bondali', 'Foni Jarrol', 'Foni Kansala']),
('Lower River Region', ARRAY['Jarra Central', 'Jarra East', 'Jarra West', 'Kiang Central', 'Kiang East', 'Kiang West']),
('North Bank Region', ARRAY['Lower Niumi', 'Upper Niumi', 'Jokadu', 'Lower Baddibu', 'Central Baddibu', 'Upper Baddibu']),
('Central River Region', ARRAY['Niamina Dankunku', 'Niamina East', 'Niamina West', 'Niani', 'Sami', 'Nianija']),
('Upper River Region', ARRAY['Fulladu East', 'Fulladu West', 'Sandu', 'Wuli East', 'Wuli West', 'Kantora']);

-- Create update trigger for updated_at
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_taxpayers_updated_at BEFORE UPDATE
    ON raw.taxpayers FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();