# Silver Layer Transformations

This directory contains silver layer transformation notebooks.

The main transformations are implemented in the Delta Live Tables pipelines located in `../pipelines/`.

## Available Transformations

- **bronze_to_silver_customers.py**: Customer dimension with SCD Type 2
- **bronze_to_silver_policies.py**: Policy fact table transformations
- **bronze_to_silver_claims.py**: Claims fact table with fraud scoring
- **bronze_to_silver_agents.py**: Agent dimension with hierarchy
- **bronze_to_silver_payments.py**: Payment fact table transformations

## Running Transformations

Transformations run automatically as part of the DLT pipeline:
```bash
databricks bundle run insurance_etl_full_refresh -t dev
```

Or trigger the DLT pipeline directly:
```bash
databricks pipelines start --pipeline-id <pipeline-id>
```

