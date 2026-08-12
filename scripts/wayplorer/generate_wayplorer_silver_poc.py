import argparse
import json
import random
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pandas as pd


BASE_MODULES = ["ACCOUNTS", "LEADS", "OPPORTUNITIES", "CALLS", "EMAILS", "TASKS"]
ACTIVITY_MODULES = {"CALLS", "EMAILS", "TASKS"}


@dataclass(frozen=True)
class TenantConfig:
    organization_id: str
    tenant_name: str
    domain: str
    company_size: str
    employee_band: str
    application_id: str
    base_records: int
    extra_fields: dict[str, list]


TENANTS = [
    TenantConfig(
        "org_fintech_nova",
        "Nova Ledger",
        "FinTech",
        "Enterprise",
        "1000_plus",
        "app_nova",
        34,
        {
            "risk_tier": ["low", "medium", "high"],
            "regulatory_region": ["NA", "EU", "APAC"],
        },
    ),
    TenantConfig(
        "org_healthops_pulse",
        "Pulse Care Ops",
        "HealthTech",
        "Mid-Market",
        "200_999",
        "app_pulse",
        24,
        {
            "care_segment": ["provider", "payer", "clinic"],
            "hipaa_review_status": ["pending", "approved", "renewal_due"],
        },
    ),
    TenantConfig(
        "org_retailmesh_peak",
        "Peak Retail Mesh",
        "Retail",
        "SMB",
        "50_199",
        "app_peak",
        18,
        {
            "store_count_band": ["1_10", "11_50", "51_plus"],
            "seasonality_profile": ["holiday_heavy", "balanced", "regional"],
        },
    ),
    TenantConfig(
        "org_edutech_orbit",
        "Orbit Campus Cloud",
        "EdTech",
        "Growth",
        "200_999",
        "app_orbit",
        22,
        {
            "student_band": ["lt_1000", "1000_10000", "10000_plus"],
            "delivery_model": ["online", "hybrid", "campus"],
        },
    ),
    TenantConfig(
        "org_logisticsforge_axis",
        "Axis Logistics Forge",
        "Logistics",
        "Enterprise",
        "1000_plus",
        "app_axis",
        30,
        {
            "fleet_band": ["regional", "national", "global"],
            "shipment_mix": ["parcel", "freight", "mixed"],
        },
    ),
]


STAGES = {
    "LEADS": ["NEW", "WORKING", "QUALIFIED", "NURTURE", "CONVERTED"],
    "OPPORTUNITIES": ["DISCOVERY", "VALIDATION", "PROPOSAL", "NEGOTIATION", "CLOSED_WON", "CLOSED_LOST"],
    "ACCOUNTS": ["ONBOARDING", "ACTIVE", "AT_RISK", "RENEWAL"],
}

OWNERS = ["u_anna", "u_ben", "u_chris", "u_dina", "u_eli", "u_faye"]
TEAMS = ["team_revops", "team_sales", "team_success", "team_growth"]
TERRITORIES = ["NA_WEST", "NA_EAST", "EMEA", "APAC"]
LEAD_SOURCES = ["Organic", "Paid", "Referral", "Partner", "Outbound"]
TASK_STATUSES = ["OPEN", "IN_PROGRESS", "DONE"]
EMAIL_OUTCOMES = ["SENT", "OPENED", "REPLIED"]
CALL_OUTCOMES = ["CONNECTED", "VOICEMAIL", "NO_ANSWER"]


def random_dt(base_time: datetime, max_days_back: int = 30) -> datetime:
    days_back = random.randint(0, max_days_back)
    hours_back = random.randint(0, 23)
    minutes_back = random.randint(0, 59)
    return base_time - timedelta(days=days_back, hours=hours_back, minutes=minutes_back)


def enrich_extras(tenant: TenantConfig) -> dict[str, str]:
    return {key: random.choice(values) for key, values in tenant.extra_fields.items()}


def build_records_for_tenant(tenant: TenantConfig, base_time: datetime) -> tuple[list[dict], list[dict], list[dict]]:
    records_rows: list[dict] = []
    activity_rows: list[dict] = []
    kpi_rows: list[dict] = []

    account_ids = [f"{tenant.organization_id}_acct_{i:04d}" for i in range(1, tenant.base_records + 1)]
    lead_ids = [f"{tenant.organization_id}_lead_{i:04d}" for i in range(1, tenant.base_records + 6)]
    opp_ids = [f"{tenant.organization_id}_opp_{i:04d}" for i in range(1, tenant.base_records + 3)]

    for idx, account_id in enumerate(account_ids, start=1):
        ts = random_dt(base_time)
        extras = enrich_extras(tenant)
        records_rows.append(
            {
                "organization_id": tenant.organization_id,
                "tenant_name": tenant.tenant_name,
                "tenant_domain": tenant.domain,
                "tenant_company_size": tenant.company_size,
                "application_id": tenant.application_id,
                "module_key": "ACCOUNTS",
                "record_id": account_id,
                "resolved_record_id": account_id,
                "resolved_owner_id": random.choice(OWNERS),
                "resolved_team_id": random.choice(TEAMS),
                "resolved_territory_id": random.choice(TERRITORIES),
                "resolved_stage_value": random.choice(STAGES["ACCOUNTS"]),
                "schema_version": "v2",
                "effective_schema_hash": f"esh_{tenant.organization_id}_acct_v2",
                "cdc_timestamp": ts.isoformat(),
                "operation_type": random.choice(["INSERT", "UPDATE"]),
                "record_name": f"{tenant.tenant_name} Account {idx}",
                "employee_band": tenant.employee_band,
                "annual_revenue_usd": random.randint(200_000, 25_000_000),
                "health_score": round(random.uniform(42, 96), 2),
                "extras_json": json.dumps(extras, sort_keys=True),
                "pipeline_run_id": base_time.strftime("%Y%m%dT%H%M%S"),
            }
        )

    for idx, lead_id in enumerate(lead_ids, start=1):
        ts = random_dt(base_time)
        extras = enrich_extras(tenant)
        records_rows.append(
            {
                "organization_id": tenant.organization_id,
                "tenant_name": tenant.tenant_name,
                "tenant_domain": tenant.domain,
                "tenant_company_size": tenant.company_size,
                "application_id": tenant.application_id,
                "module_key": "LEADS",
                "record_id": lead_id,
                "resolved_record_id": lead_id,
                "resolved_owner_id": random.choice(OWNERS),
                "resolved_team_id": random.choice(TEAMS),
                "resolved_territory_id": random.choice(TERRITORIES),
                "resolved_stage_value": random.choice(STAGES["LEADS"]),
                "schema_version": "v2",
                "effective_schema_hash": f"esh_{tenant.organization_id}_lead_v2",
                "cdc_timestamp": ts.isoformat(),
                "operation_type": random.choice(["INSERT", "UPDATE"]),
                "record_name": f"Lead {idx} {tenant.domain}",
                "lead_source": random.choice(LEAD_SOURCES),
                "lead_score_hint": round(random.uniform(15, 95), 2),
                "extras_json": json.dumps(extras, sort_keys=True),
                "pipeline_run_id": base_time.strftime("%Y%m%dT%H%M%S"),
            }
        )

    for idx, opp_id in enumerate(opp_ids, start=1):
        ts = random_dt(base_time)
        extras = enrich_extras(tenant)
        records_rows.append(
            {
                "organization_id": tenant.organization_id,
                "tenant_name": tenant.tenant_name,
                "tenant_domain": tenant.domain,
                "tenant_company_size": tenant.company_size,
                "application_id": tenant.application_id,
                "module_key": "OPPORTUNITIES",
                "record_id": opp_id,
                "resolved_record_id": opp_id,
                "resolved_owner_id": random.choice(OWNERS),
                "resolved_team_id": random.choice(TEAMS),
                "resolved_territory_id": random.choice(TERRITORIES),
                "resolved_stage_value": random.choice(STAGES["OPPORTUNITIES"]),
                "schema_version": "v2",
                "effective_schema_hash": f"esh_{tenant.organization_id}_opp_v2",
                "cdc_timestamp": ts.isoformat(),
                "operation_type": random.choice(["INSERT", "UPDATE"]),
                "record_name": f"Opportunity {idx} {tenant.domain}",
                "amount_usd": random.randint(5000, 450000),
                "forecast_category": random.choice(["Commit", "Best Case", "Pipeline"]),
                "extras_json": json.dumps(extras, sort_keys=True),
                "pipeline_run_id": base_time.strftime("%Y%m%dT%H%M%S"),
            }
        )

    activity_count = tenant.base_records * 5
    for idx in range(1, activity_count + 1):
        module_key = random.choice(sorted(ACTIVITY_MODULES))
        ts = random_dt(base_time, max_days_back=21)
        target_lead = random.choice(lead_ids)
        target_account = random.choice(account_ids)
        base_row = {
            "organization_id": tenant.organization_id,
            "tenant_name": tenant.tenant_name,
            "tenant_domain": tenant.domain,
            "application_id": tenant.application_id,
            "module_key": module_key,
            "record_id": f"{tenant.organization_id}_{module_key.lower()}_{idx:05d}",
            "resolved_record_id": f"{tenant.organization_id}_{module_key.lower()}_{idx:05d}",
            "resolved_owner_id": random.choice(OWNERS),
            "resolved_team_id": random.choice(TEAMS),
            "resolved_territory_id": random.choice(TERRITORIES),
            "resolved_stage_value": None,
            "schema_version": "v2",
            "effective_schema_hash": f"esh_{tenant.organization_id}_{module_key.lower()}_v2",
            "cdc_timestamp": ts.isoformat(),
            "activity_date": ts.date().isoformat(),
            "operation_type": random.choice(["INSERT", "UPDATE"]),
            "account_id": target_account,
            "lead_id": target_lead,
            "pipeline_run_id": base_time.strftime("%Y%m%dT%H%M%S"),
        }
        if module_key == "CALLS":
            base_row.update(
                {
                    "activity_type": "CALL",
                    "duration_seconds": random.randint(60, 1800),
                    "outcome": random.choice(CALL_OUTCOMES),
                    "extras_json": json.dumps(enrich_extras(tenant), sort_keys=True),
                }
            )
        elif module_key == "EMAILS":
            base_row.update(
                {
                    "activity_type": "EMAIL",
                    "duration_seconds": 0,
                    "outcome": random.choice(EMAIL_OUTCOMES),
                    "extras_json": json.dumps(enrich_extras(tenant), sort_keys=True),
                }
            )
        else:
            base_row.update(
                {
                    "activity_type": "TASK",
                    "duration_seconds": random.randint(120, 2400),
                    "outcome": random.choice(TASK_STATUSES),
                    "extras_json": json.dumps(enrich_extras(tenant), sort_keys=True),
                }
            )
        activity_rows.append(base_row)

    for day_offset in range(0, 14):
        kpi_dt = (base_time.date() - timedelta(days=day_offset))
        kpi_rows.append(
            {
                "organization_id": tenant.organization_id,
                "tenant_name": tenant.tenant_name,
                "tenant_domain": tenant.domain,
                "tenant_company_size": tenant.company_size,
                "kpi_date": kpi_dt.isoformat(),
                "pipeline_created_count": random.randint(4, 28),
                "qualified_lead_count": random.randint(3, 24),
                "active_opportunity_count": random.randint(5, 32),
                "stage_conversion_rate": round(random.uniform(0.12, 0.78), 4),
                "avg_stage_dwell_days": round(random.uniform(2.5, 19.0), 2),
                "activity_volume": random.randint(15, 120),
                "goal_attainment_ratio": round(random.uniform(0.42, 1.18), 4),
                "pipeline_coverage_ratio": round(random.uniform(0.55, 1.65), 4),
                "kpi_readiness_status": random.choice(["READY", "PARTIAL", "BLOCKED"]),
                "pipeline_run_id": base_time.strftime("%Y%m%dT%H%M%S"),
            }
        )

    return records_rows, activity_rows, kpi_rows


def write_partitioned_parquet(df: pd.DataFrame, root: Path, partitions: list[str]) -> None:
    root.mkdir(parents=True, exist_ok=True)
    df.to_parquet(root, engine="pyarrow", index=False, partition_cols=partitions)


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate a multi-tenant Wayplorer Silver-layer PoC.")
    parser.add_argument("--output-dir", default="data/silver_poc", help="Local Silver PoC output directory.")
    parser.add_argument(
        "--base-timestamp",
        default=datetime.now(UTC).replace(microsecond=0).isoformat(),
        help="Base timestamp for data generation.",
    )
    args = parser.parse_args()

    random.seed(42)
    base_time = datetime.fromisoformat(args.base_timestamp)
    output_dir = Path(args.output_dir)

    all_records: list[dict] = []
    all_activities: list[dict] = []
    all_kpis: list[dict] = []

    for tenant in TENANTS:
        records_rows, activity_rows, kpi_rows = build_records_for_tenant(tenant, base_time)
        all_records.extend(records_rows)
        all_activities.extend(activity_rows)
        all_kpis.extend(kpi_rows)

    records_df = pd.DataFrame(all_records)
    records_df["event_date"] = pd.to_datetime(records_df["cdc_timestamp"]).dt.date.astype(str)
    records_df["year"] = pd.to_datetime(records_df["cdc_timestamp"]).dt.year
    records_df["month"] = pd.to_datetime(records_df["cdc_timestamp"]).dt.month
    records_df["day"] = pd.to_datetime(records_df["cdc_timestamp"]).dt.day

    activities_df = pd.DataFrame(all_activities)
    activities_df["year"] = pd.to_datetime(activities_df["cdc_timestamp"]).dt.year
    activities_df["month"] = pd.to_datetime(activities_df["cdc_timestamp"]).dt.month
    activities_df["day"] = pd.to_datetime(activities_df["cdc_timestamp"]).dt.day

    kpis_df = pd.DataFrame(all_kpis)

    write_partitioned_parquet(
        records_df,
        output_dir / "silver_records_resolved",
        ["organization_id", "module_key", "year", "month", "day"],
    )
    write_partitioned_parquet(
        activities_df,
        output_dir / "silver_activity_resolved",
        ["organization_id", "module_key", "year", "month", "day"],
    )
    write_partitioned_parquet(
        kpis_df,
        output_dir / "silver_kpi_daily_summary",
        ["organization_id", "kpi_date"],
    )

    print(f"Generated records rows: {len(records_df)}")
    print(f"Generated activity rows: {len(activities_df)}")
    print(f"Generated KPI rows: {len(kpis_df)}")
    print(f"Output written to: {output_dir}")


if __name__ == "__main__":
    main()
