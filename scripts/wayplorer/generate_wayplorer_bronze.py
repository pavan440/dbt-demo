import argparse
import json
import random
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Iterable


CLIENT_IDS = ["client_alpha", "client_beta", "client_gamma"]
INDUSTRIES = ["Travel", "Hospitality", "SaaS", "Retail"]
LEAD_SOURCES = ["Web", "Partner", "Outbound", "Referral"]
OPPORTUNITY_STAGES = ["Prospecting", "Qualified", "Proposal", "Negotiation", "Closed Won"]
CONTRACT_STATUSES = ["ACTIVE", "EXPIRED", "PENDING"]
CALL_OUTCOMES = ["CONNECTED", "VOICEMAIL", "NO_ANSWER"]
EMAIL_STATUSES = ["SENT", "OPENED", "REPLIED"]
TASK_STATUSES = ["OPEN", "IN_PROGRESS", "DONE"]
TASK_PRIORITIES = ["LOW", "MEDIUM", "HIGH"]


@dataclass
class BaseRecord:
    applicationId: str
    cdc_timestamp: str
    operationType: str
    run_date: str


def iso_ts(base_time: datetime, minutes_offset: int) -> str:
    return (base_time + timedelta(minutes=minutes_offset)).replace(microsecond=0).isoformat()


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def partitioned_file(base_dir: Path, entity: str, ts: datetime, batch_id: str) -> Path:
    return (
        base_dir
        / entity
        / f"year={ts:%Y}"
        / f"month={ts:%m}"
        / f"day={ts:%d}"
        / f"hour={ts:%H}"
        / f"{entity}_{batch_id}.json"
    )


def write_records(path: Path, records: Iterable[dict]) -> None:
    ensure_parent(path)
    with path.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record) + "\n")


def build_accounts(base_time: datetime, sample_size: int) -> list[dict]:
    rows = []
    for i in range(1, sample_size + 1):
        ts = iso_ts(base_time, i)
        rows.append(
            {
                **asdict(BaseRecord("wayplorer_crm", ts, "UPSERT", base_time.date().isoformat())),
                "accountId": f"ACC{i:04d}",
                "clientId": random.choice(CLIENT_IDS),
                "accountName": f"Wayplorer Account {i}",
                "industry": random.choice(INDUSTRIES),
                "annualRevenue": random.randint(200_000, 12_000_000),
                "numberOfEmployees": random.randint(10, 1800),
                "accountType": random.choice(["Prospect", "Customer"]),
            }
        )
    return rows


def build_leads(base_time: datetime, sample_size: int) -> list[dict]:
    rows = []
    for i in range(1, sample_size + 1):
        ts = iso_ts(base_time, 100 + i)
        rows.append(
            {
                **asdict(BaseRecord("wayplorer_crm", ts, "UPSERT", base_time.date().isoformat())),
                "leadId": f"LED{i:04d}",
                "accountId": f"ACC{((i - 1) % sample_size) + 1:04d}",
                "clientId": random.choice(CLIENT_IDS),
                "firstName": f"Lead{i}",
                "lastName": "User",
                "email": f"lead{i}@wayplorer.test",
                "company": f"Wayplorer Account {((i - 1) % sample_size) + 1}",
                "leadSource": random.choice(LEAD_SOURCES),
                "leadStatus": random.choice(["OPEN", "WORKING", "QUALIFIED", "CONVERTED"]),
                "createdDate": ts,
            }
        )
    return rows


def build_opportunities(base_time: datetime, sample_size: int) -> list[dict]:
    rows = []
    for i in range(1, sample_size + 1):
        ts = iso_ts(base_time, 200 + i)
        rows.append(
            {
                **asdict(BaseRecord("wayplorer_crm", ts, "UPSERT", base_time.date().isoformat())),
                "opportunityId": f"OPP{i:04d}",
                "leadId": f"LED{((i - 1) % sample_size) + 1:04d}",
                "accountId": f"ACC{((i - 1) % sample_size) + 1:04d}",
                "clientId": random.choice(CLIENT_IDS),
                "stage": random.choice(OPPORTUNITY_STAGES),
                "probability": random.choice([20, 40, 60, 80, 100]),
                "amount": random.randint(5000, 250000),
                "createdDate": ts,
            }
        )
    return rows


def build_contracts(base_time: datetime, sample_size: int) -> list[dict]:
    rows = []
    for i in range(1, sample_size + 1):
        ts = iso_ts(base_time, 300 + i)
        start_date = (base_time - timedelta(days=random.randint(15, 240))).date().isoformat()
        end_date = (base_time + timedelta(days=random.randint(30, 365))).date().isoformat()
        rows.append(
            {
                **asdict(BaseRecord("wayplorer_crm", ts, "UPSERT", base_time.date().isoformat())),
                "contractId": f"CON{i:04d}",
                "accountId": f"ACC{((i - 1) % sample_size) + 1:04d}",
                "clientId": random.choice(CLIENT_IDS),
                "contractValue": random.randint(10_000, 400_000),
                "contractStatus": random.choice(CONTRACT_STATUSES),
                "contractStartDate": start_date,
                "contractEndDate": end_date,
            }
        )
    return rows


def build_calls(base_time: datetime, sample_size: int) -> list[dict]:
    rows = []
    for i in range(1, sample_size * 2 + 1):
        ts = iso_ts(base_time, 400 + i)
        rows.append(
            {
                **asdict(BaseRecord("wayplorer_crm", ts, "UPSERT", base_time.date().isoformat())),
                "callId": f"CAL{i:05d}",
                "accountId": f"ACC{((i - 1) % sample_size) + 1:04d}",
                "leadId": f"LED{((i - 1) % sample_size) + 1:04d}",
                "clientId": random.choice(CLIENT_IDS),
                "direction": random.choice(["INBOUND", "OUTBOUND"]),
                "outcome": random.choice(CALL_OUTCOMES),
                "durationSeconds": random.randint(45, 2400),
                "callStartTime": ts,
            }
        )
    return rows


def build_emails(base_time: datetime, sample_size: int) -> list[dict]:
    rows = []
    for i in range(1, sample_size * 3 + 1):
        ts = iso_ts(base_time, 500 + i)
        rows.append(
            {
                **asdict(BaseRecord("wayplorer_crm", ts, "UPSERT", base_time.date().isoformat())),
                "emailId": f"EML{i:05d}",
                "accountId": f"ACC{((i - 1) % sample_size) + 1:04d}",
                "leadId": f"LED{((i - 1) % sample_size) + 1:04d}",
                "clientId": random.choice(CLIENT_IDS),
                "subject": f"Wayplorer follow-up {i}",
                "emailStatus": random.choice(EMAIL_STATUSES),
                "sentTime": ts,
            }
        )
    return rows


def build_tasks(base_time: datetime, sample_size: int) -> list[dict]:
    rows = []
    for i in range(1, sample_size * 2 + 1):
        ts = iso_ts(base_time, 600 + i)
        due_date = (base_time + timedelta(days=random.randint(1, 30))).date().isoformat()
        completed_at = None
        status = random.choice(TASK_STATUSES)
        if status == "DONE":
            completed_at = iso_ts(base_time, 700 + i)
        rows.append(
            {
                **asdict(BaseRecord("wayplorer_crm", ts, "UPSERT", base_time.date().isoformat())),
                "taskId": f"TSK{i:05d}",
                "accountId": f"ACC{((i - 1) % sample_size) + 1:04d}",
                "leadId": f"LED{((i - 1) % sample_size) + 1:04d}",
                "clientId": random.choice(CLIENT_IDS),
                "taskSubject": f"Wayplorer follow-up task {i}",
                "taskStatus": status,
                "taskPriority": random.choice(TASK_PRIORITIES),
                "dueDate": due_date,
                "completedAt": completed_at,
            }
        )
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate sample Wayplorer Bronze CDC files.")
    parser.add_argument("--output-dir", default="data/bronze", help="Local Bronze root directory.")
    parser.add_argument("--sample-size", type=int, default=25, help="Base sample size per entity.")
    parser.add_argument(
        "--base-timestamp",
        default=datetime.now(UTC).replace(microsecond=0).isoformat(),
        help="ISO timestamp used to derive partition folders.",
    )
    args = parser.parse_args()

    base_time = datetime.fromisoformat(args.base_timestamp)
    output_dir = Path(args.output_dir)
    batch_id = base_time.strftime("%Y%m%dT%H%M%S")

    entity_builders = {
        "account": build_accounts(base_time, args.sample_size),
        "lead": build_leads(base_time, args.sample_size),
        "opportunity": build_opportunities(base_time, args.sample_size),
        "contract": build_contracts(base_time, args.sample_size),
        "call": build_calls(base_time, args.sample_size),
        "email": build_emails(base_time, args.sample_size),
        "task": build_tasks(base_time, args.sample_size),
    }

    for entity, records in entity_builders.items():
        target = partitioned_file(output_dir, entity, base_time, batch_id)
        write_records(target, records)
        print(f"Wrote {len(records)} records to {target}")


if __name__ == "__main__":
    main()
