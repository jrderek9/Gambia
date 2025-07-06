# GTA Data Warehouse Deployment Guide

## Prerequisites

- Docker and Docker Compose installed
- Minimum 8GB RAM, 20GB storage
- Ubuntu 20.04+ or similar Linux distribution
- Basic knowledge of command line

## Quick Start (5 minutes)

1. **Clone the repository**
   ```bash
   git clone https://github.com/gta/data-warehouse-demo.git
   cd gta-warehouse-demo
   ```

2. **Start all services**
   ```bash
   docker-compose up -d
   ```

3. **Wait for services to initialize** (3-5 minutes)
   ```bash
   docker-compose ps  # Check all services are running
   ```

4. **Access the services**
   - Airflow: http://localhost:8080 (admin/admin)
   - Metabase: http://localhost:3000
   - PostgreSQL: localhost:5432 (gta_admin/gta_secure_pass)

## Step-by-Step Deployment

### 1. System Preparation

```bash
# Update system
sudo apt update && sudo apt upgrade -y

# Install Docker
curl -fsSL https://get.docker.com -o get-docker.sh
sudo sh get-docker.sh

# Install Docker Compose
sudo curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose

# Verify installation
docker --version
docker-compose --version
```

### 2. Configure Environment

```bash
# Create environment file
cp .env.example .env

# Edit configuration (optional)
nano .env
```

Key configurations:
- `POSTGRES_PASSWORD`: Change for production
- `AIRFLOW_FERNET_KEY`: Generate new key for production
- `METABASE_SECRET_KEY`: Generate new key for production

### 3. Initialize Database

```bash
# Start only PostgreSQL first
docker-compose up -d postgres

# Wait for PostgreSQL to be ready
docker-compose exec postgres pg_isready

# The init script runs automatically, but you can verify:
docker-compose exec postgres psql -U gta_admin -d gta_warehouse -c "\dt raw.*"
```

### 4. Generate Sample Data

```bash
# Run data generation script
docker-compose exec airflow-webserver python /opt/airflow/scripts/generate_synthetic_data.py

# This creates 50,000 taxpayers with 2 years of transaction history
```

### 5. Initialize Airflow

```bash
# Start Airflow services
docker-compose up -d airflow-webserver airflow-scheduler

# Wait for initialization (check logs)
docker-compose logs -f airflow-webserver

# Access Airflow UI
# Username: admin
# Password: admin
```

### 6. Run dbt Models

```bash
# Execute dbt transformations
docker-compose exec dbt dbt run --project-dir gta_demo --profiles-dir /usr/app/dbt

# Test dbt models
docker-compose exec dbt dbt test --project-dir gta_demo --profiles-dir /usr/app/dbt
```

### 7. Configure Metabase

1. Access Metabase at http://localhost:3000
2. Complete initial setup:
   - Email: admin@gta.gm
   - Password: [choose secure password]
   - Database: PostgreSQL
   - Host: postgres
   - Port: 5432
   - Database name: gta_warehouse
   - Username: gta_admin
   - Password: gta_secure_pass

3. Import pre-built dashboards:
   ```bash
   # Dashboards are in /dashboards/metabase_setup.sql
   # Copy queries to create cards and dashboards
   ```

### 8. Schedule Airflow DAGs

1. In Airflow UI, enable DAGs:
   - `gta_data_pipeline` (daily at 2 AM)
   - `ml_fraud_detection` (weekly on Mondays)
   - `revenue_forecasting` (weekly on Mondays)

2. Trigger initial runs:
   ```bash
   # Via UI or command line
   docker-compose exec airflow-webserver airflow dags trigger gta_data_pipeline
   ```

## Production Considerations

### Security

1. **Change all default passwords**
   ```bash
   # PostgreSQL
   ALTER USER gta_admin WITH PASSWORD 'new_secure_password';
   
   # Update docker-compose.yml and connection strings
   ```

2. **Enable SSL/TLS**
   - Add SSL certificates to nginx configuration
   - Enable HTTPS for all web services

3. **Network Security**
   - Use firewall rules to restrict access
   - Implement VPN for remote access

### Backup and Recovery

1. **Database Backup**
   ```bash
   # Create backup script
   docker-compose exec postgres pg_dump -U gta_admin gta_warehouse > backup_$(date +%Y%m%d).sql
   
   # Schedule daily backups via cron
   0 2 * * * /path/to/backup_script.sh
   ```

2. **Application State**
   ```bash
   # Backup Metabase dashboards
   docker-compose exec metabase-app tar -czf /tmp/metabase-backup.tar.gz /metabase.db
   
   # Backup Airflow configurations
   docker-compose exec airflow-webserver tar -czf /tmp/airflow-backup.tar.gz /opt/airflow/dags /opt/airflow/logs
   ```

### Monitoring

1. **Service Health Checks**
   ```bash
   # Create monitoring script
   #!/bin/bash
   services=("postgres" "airflow-webserver" "metabase")
   for service in "${services[@]}"; do
     if ! docker-compose ps $service | grep -q "Up"; then
       echo "Service $service is down!"
       # Send alert
     fi
   done
   ```

2. **Resource Monitoring**
   - CPU usage: `docker stats`
   - Disk space: `df -h`
   - Memory: `free -m`

### Scaling

1. **Horizontal Scaling**
   - Add Airflow workers for parallel processing
   - Use PostgreSQL replication for read scaling

2. **Vertical Scaling**
   - Increase container resource limits in docker-compose.yml
   - Adjust PostgreSQL configuration for larger datasets

## Troubleshooting

### Common Issues

1. **Services not starting**
   ```bash
   # Check logs
   docker-compose logs [service-name]
   
   # Restart services
   docker-compose restart [service-name]
   ```

2. **Database connection errors**
   ```bash
   # Test connection
   docker-compose exec postgres psql -U gta_admin -d gta_warehouse
   
   # Check network
   docker network ls
   docker network inspect gta-warehouse-demo_gta_network
   ```

3. **Airflow DAG errors**
   ```bash
   # Check DAG syntax
   docker-compose exec airflow-webserver airflow dags list
   
   # Test specific DAG
   docker-compose exec airflow-webserver airflow dags test gta_data_pipeline
   ```

4. **Metabase visualization issues**
   - Clear browser cache
   - Check data source connections
   - Verify query permissions

### Support Contacts

- Technical Issues: tech-support@gta-demo.com
- Documentation: docs@gta-demo.com
- Emergency: +220-xxx-xxxx

## Maintenance Schedule

- **Daily**: Automated backups at 2 AM
- **Weekly**: ML model retraining (Mondays)
- **Monthly**: Security updates and patches
- **Quarterly**: Performance optimization review

## Training Resources

1. **Video Tutorials** (in /docs/videos/):
   - 01_getting_started.mp4
   - 02_dashboard_creation.mp4
   - 03_fraud_detection.mp4
   - 04_troubleshooting.mp4

2. **User Manuals** (in /docs/manuals/):
   - Analyst_Guide.pdf
   - Administrator_Manual.pdf
   - Developer_Documentation.pdf

3. **Quick Reference Cards** (in /docs/quick-ref/):
   - Dashboard_Shortcuts.pdf
   - SQL_Query_Templates.pdf
   - Common_Tasks.pdf

## Migration from Current System

### Phase 1: Parallel Running (Weeks 1-2)
- Deploy new system alongside existing
- Import historical data
- Verify data accuracy

### Phase 2: Pilot Testing (Weeks 3-4)
- Select pilot team (5-10 users)
- Daily validation against old system
- Gather feedback and adjust

### Phase 3: Gradual Rollout (Weeks 5-6)
- Department by department migration
- Training sessions for each group
- Maintain old system as backup

### Phase 4: Full Migration (Weeks 7-8)
- Complete cutover
- Archive old system
- Post-migration support

## Cost Calculator

### Infrastructure Costs (Annual)
- Cloud hosting: D 120,000
- Backup storage: D 24,000
- SSL certificates: D 6,000
- **Total: D 150,000**

### Savings (Annual)
- License fees eliminated: D 400,000
- Reduced manual work (30 FTEs @ 50%): D 900,000
- Improved collections (15% of D 50M): D 7,500,000
- **Total Savings: D 8,800,000**

### ROI
- Investment payback: 2 months
- Net benefit Year 1: D 8,650,000
- 5-year NPV: D 40,000,000+

## Next Steps

1. Review and approve deployment plan
2. Provision infrastructure
3. Schedule training sessions
4. Begin phased rollout

Remember: This is a journey, not a destination. The system will continue to improve with more data and user feedback.