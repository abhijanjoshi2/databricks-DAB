# Sales Analytics DAB

A simple Databricks Asset Bundle for sales data analytics.

## Setup

1. **Set GitHub Secrets:**
   - `DATABRICKS_HOST`: Your Databricks workspace URL
   - `DATABRICKS_TOKEN`: Your Databricks personal access token

2. **Update databricks.yml:**
   - Replace workspace host URLs with your actual workspace

3. **Deploy:**
   - Push to `develop` branch for dev deployment
   - Push to `main` branch for prod deployment

## Local Development

```bash
# Validate bundle
databricks bundle validate --target dev

# Deploy to dev
databricks bundle deploy --target dev

# Run the job
databricks bundle run sales_analytics_job --target dev