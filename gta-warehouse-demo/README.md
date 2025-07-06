# GTA Data Warehouse Demo

A comprehensive data warehouse proof-of-concept for the Gambian Tax Authority using open-source tools.

## Quick Start

1. Clone this repository
2. Run `docker-compose up -d`
3. Access the services:
   - Airflow: http://localhost:8080 (admin/admin)
   - Metabase: http://localhost:3000
   - PostgreSQL: localhost:5432 (gta_admin/gta_secure_pass)

## Architecture

- **PostgreSQL**: Main data warehouse
- **Apache Airflow**: Orchestration and pipeline management
- **dbt Core**: Data transformation
- **Metabase**: Business intelligence dashboards
- **Python**: Data ingestion and ML models
- **Docker**: Containerization

## Demo Features

1. Executive Revenue Dashboard
2. ML-based Fraud Detection System
3. Taxpayer 360° View
4. Self-Service Analytics

## Project Structure

```
gta-warehouse-demo/
├── airflow/          # Airflow DAGs and configurations
├── dbt/              # dbt models and transformations
├── ml/               # Machine learning models
├── scripts/          # Data generation and utility scripts
├── dashboards/       # Metabase dashboard exports
├── data/             # Sample data files
├── docker/           # Docker configurations
└── docs/             # Documentation
```


Based on the Docker setup I created, here are the PostgreSQL connection details for Metabase:
  Display name: GTA Data Warehouse

  Host: postgres

  Port: 5432

  Database name: gta_warehouse

  Username: gta_admin

  Password: gta_secure_pass

  Schemas: All

  These credentials are defined in the docker-compose.yml file. If Metabase is running inside Docker (which it should be based on our setup), use postgres as the host. If you're accessing from
   outside the Docker network, use localhost.

> what is the display name for the postgresql

⏺ For the display name in Metabase, you can use:

  Display name: GTA Data Warehouse

  This is a user-friendly name that will appear in Metabase's interface. You could also use alternatives like:
  - GTA Tax Database
  - Gambian Tax Authority DW
  - GTA Analytics Database

  The display name is just for identification within Metabase and doesn't affect the connection.