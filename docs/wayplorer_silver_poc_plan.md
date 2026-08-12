# Wayplorer Silver-First PoC Plan

This PoC starts from the canonical Wayplorer Silver layer and treats Snowflake as the downstream analytics serving layer.

## KPI Layer

Yes, the Wayplorer design includes KPI intelligence, but KPI serving belongs to the deterministic Gold/intelligence path.

For this PoC:

- Silver will include resolved CRM records and activities.
- Silver will also include a KPI-ready daily summary table to make downstream Snowflake/dbt work easier.
- Snowflake will ingest Silver tables first.
- Gold KPI marts should be built in Snowflake dbt after ingestion.

## Multi-Tenant Rules

- Tenant boundary: `organization_id`
- Module boundary: `module_key`
- Optional segmentation: `application_id`
- Tenants may have extra custom fields beyond the base schema.
- Unknown/custom tenant fields are preserved in `extras_json`.

## Five Synthetic Tenants

The PoC uses five tenants with different domains and company sizes:

1. `org_fintech_nova` - FinTech Enterprise
2. `org_healthops_pulse` - HealthTech Mid-Market
3. `org_retailmesh_peak` - Retail SMB
4. `org_edutech_orbit` - EdTech Growth
5. `org_logisticsforge_axis` - Logistics Enterprise

Each tenant has:

- different volume
- different industry
- different custom fields
- different stage and activity distributions

## Silver Tables Generated

- `silver_records_resolved`
- `silver_activity_resolved`
- `silver_kpi_daily_summary`

## Local Output Layout

```text
data/silver_poc/
  silver_records_resolved/
    organization_id=<org>/module_key=<module>/year=YYYY/month=MM/day=DD/part-*.parquet
  silver_activity_resolved/
    organization_id=<org>/module_key=<module>/year=YYYY/month=MM/day=DD/part-*.parquet
  silver_kpi_daily_summary/
    organization_id=<org>/kpi_date=YYYY-MM-DD/part-*.parquet
```

## Target S3 Layout

```text
s3://wayplorer-data-lake-449/wayplorerdbtpoc/
  silver_records_resolved/
  silver_activity_resolved/
  silver_kpi_daily_summary/
```

## Snowflake Ingestion Pattern

1. Generate local Silver PoC Parquet files.
2. Upload them to the Silver S3 prefix using the `learning` AWS profile.
3. Create Snowflake external stage(s) on the Silver prefix.
4. `COPY INTO` Snowflake landing tables.
5. Build Snowflake dbt staging/core/gold models on top.

