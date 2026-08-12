# Wayplorer Ingestion Concept

This starter keeps the Wayplorer flow aligned with the architecture documented in your Bronze, Silver, and Gold notes.

## End-to-End Shape

1. Generate sample Wayplorer CDC-style files for core entities.
2. Land those files in S3 Bronze by entity and event time.
3. Run a Glue/Spark job to coerce the raw JSON into Iceberg-backed Silver tables.
4. Build Gold marts from Silver and export them to S3 as Parquet.
5. Load the Parquet exports into Snowflake either:
   - in batch mode with `COPY INTO`, or
   - in near-real-time with Snowpipe auto-ingest.

## Wayplorer Domains Included

- `account`
- `lead`
- `opportunity`
- `contract`
- `call`
- `email`

These are the core entities referenced across your Silver and Gold documents and are enough to support churn, lead-score, and sales-cycle examples.

## S3 Layout

```text
s3://<bucket>/wayplorer/
  bronze/
    account/year=YYYY/month=MM/day=DD/hour=HH/*.json
    lead/year=YYYY/month=MM/day=DD/hour=HH/*.json
    opportunity/year=YYYY/month=MM/day=DD/hour=HH/*.json
    contract/year=YYYY/month=MM/day=DD/hour=HH/*.json
    call/year=YYYY/month=MM/day=DD/hour=HH/*.json
    email/year=YYYY/month=MM/day=DD/hour=HH/*.json
  silver/
    crm/<table>/data
    crm/<table>/metadata
  gold/
    mart_customer_churn_features/client_id=<id>/feature_date=YYYY-MM-DD/*.parquet
    mart_lead_score/client_id=<id>/feature_date=YYYY-MM-DD/*.parquet
```

## Delivery Assets In This Repo

- Sample CDC generator:
  - [scripts/wayplorer/generate_wayplorer_bronze.py](/D:/DBT_snowflake/scripts/wayplorer/generate_wayplorer_bronze.py:1)
- Glue/Spark Bronze-to-Silver Iceberg loader:
  - [scripts/wayplorer/glue_bronze_to_silver_iceberg.py](/D:/DBT_snowflake/scripts/wayplorer/glue_bronze_to_silver_iceberg.py:1)
- Glue/Spark Gold export to Parquet for Snowflake:
  - [scripts/wayplorer/export_gold_to_s3_parquet.py](/D:/DBT_snowflake/scripts/wayplorer/export_gold_to_s3_parquet.py:1)
- Snowflake batch ingestion setup:
  - [snowflake/01_batch_ingest_wayplorer.sql](/D:/DBT_snowflake/snowflake/01_batch_ingest_wayplorer.sql:1)
- Snowflake Snowpipe setup:
  - [snowflake/02_realtime_snowpipe_wayplorer.sql](/D:/DBT_snowflake/snowflake/02_realtime_snowpipe_wayplorer.sql:1)

## Recommended Flow

1. Generate local sample files.
2. Upload the Bronze samples to your S3 Bronze prefix.
3. Run the Glue Iceberg loader per entity.
4. Create or refresh Gold marts in your lakehouse.
5. Export the Gold marts to S3 Parquet.
6. Use Snowflake batch mode first.
7. Add Snowpipe after the Parquet export pattern is stable.
