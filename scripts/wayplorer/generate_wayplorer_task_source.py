import argparse
import json
import random
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Iterable


CLIENT_IDS = ["client_alpha", "client_beta", "client_gamma"]
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
    parser = argparse.ArgumentParser(description="Generate sample Wayplorer task CDC files.")
    parser.add_argument("--output-dir", default="data/bronze", help="Local Bronze root directory.")
    parser.add_argument("--sample-size", type=int, default=25, help="Base sample size for task generation.")
    parser.add_argument(
        "--base-timestamp",
        default=datetime.now(UTC).replace(microsecond=0).isoformat(),
        help="ISO timestamp used to derive partition folders.",
    )
    args = parser.parse_args()

    base_time = datetime.fromisoformat(args.base_timestamp)
    output_dir = Path(args.output_dir)
    batch_id = base_time.strftime("%Y%m%dT%H%M%S")

    task_records = build_tasks(base_time, args.sample_size)
    target = partitioned_file(output_dir, "task", base_time, batch_id)
    write_records(target, task_records)
    print(f"Wrote {len(task_records)} task records to {target}")


if __name__ == "__main__":
    main()
