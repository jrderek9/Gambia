#!/bin/bash

echo "======================================"
echo "GTA Enhanced Data Warehouse Demo Setup"
echo "======================================"

# Check if docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker is not running. Please start Docker first."
    exit 1
fi

echo "✅ Docker is running"

# Stop any existing containers
echo "Stopping existing containers..."
docker-compose -f docker-compose-simple.yml down 2>/dev/null
docker-compose -f docker-compose-enhanced.yml down 2>/dev/null

# Build and start enhanced stack
echo "Starting enhanced stack..."
docker-compose -f docker-compose-enhanced.yml up -d --build

# Wait for services to be ready
echo "Waiting for services to initialize..."
sleep 30

# Check service health
echo "Checking service status..."
docker-compose -f docker-compose-enhanced.yml ps

# Generate comprehensive data
echo "Generating comprehensive demo data..."
docker-compose -f docker-compose-enhanced.yml exec dbt python /usr/app/scripts/generate_comprehensive_data.py

# Run dbt transformations
echo "Running dbt transformations..."
docker-compose -f docker-compose-enhanced.yml exec dbt dbt run --project-dir /usr/app/dbt/gta_demo --profiles-dir /usr/app/dbt

# Create analytics views
echo "Creating analytics views..."
docker-compose -f docker-compose-enhanced.yml exec -T postgres psql -U gta_admin -d gta_warehouse < scripts/create_analytics_views.sql

# Run Luigi pipeline
echo "Starting Luigi pipeline..."
docker-compose -f docker-compose-enhanced.yml exec luigi python /usr/app/luigi/gta_pipeline.py MasterPipeline --local-scheduler

echo ""
echo "======================================"
echo "✅ Demo Setup Complete!"
echo "======================================"
echo ""
echo "Access the following services:"
echo "  📊 Metabase BI: http://localhost:3000"
echo "  🚀 Streamlit Dashboard: http://localhost:8501"
echo "  🔄 Luigi Pipeline: http://localhost:8082"
echo "  🗄️ PostgreSQL: localhost:5432"
echo ""
echo "Demo Credentials:"
echo "  Database: gta_admin / gta_secure_pass"
echo ""
echo "Features:"
echo "  ✓ 10,000 taxpayers across all regions"
echo "  ✓ 2 years of transaction history"
echo "  ✓ Real-time transaction monitoring"
echo "  ✓ ML-powered fraud detection"
echo "  ✓ Predictive revenue forecasting"
echo "  ✓ Interactive dashboards"
echo ""
echo "To view logs:"
echo "  docker-compose -f docker-compose-enhanced.yml logs -f [service]"
echo ""