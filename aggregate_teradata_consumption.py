import csv
from collections import defaultdict
from decimal import Decimal
from pathlib import Path


RAW_INPUT_PATH = Path("data/raw-teradata/cba_teradata_dbql_raw.csv")
AGG_OUTPUT_PATH = Path("data/aggregated-teradata/consumption-aggregated.csv")

BATCH_ID = "td-dbql-poc-001"
SOURCE = "Teradata DBQL"
PLATFORM = "Teradata"
CONFIDENCE = "0.95"


def clean(value: str | None) -> str:
    return (value or "").strip()


def to_decimal(value: str | None) -> Decimal:
    value = clean(value)
    if not value:
        return Decimal("0")
    return Decimal(value)


def to_int(value: str | None) -> int:
    value = clean(value)
    if not value:
        return 0
    return int(float(value))


def aggregate(input_path: str | Path, output_path: str | Path) -> None:
    input_path = Path(input_path)
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    aggregated = defaultdict(
        lambda: {
            "query_count": 0,
            "first_seen": None,
            "last_seen": None,
            "total_amp_cpu_sec": Decimal("0"),
            "total_elapsed_sec": Decimal("0"),
            "total_rows_read": 0,
            "total_spool_gb": Decimal("0"),
        }
    )

    raw_rows = 0
    selected_rows = 0

    with input_path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row in reader:
            raw_rows += 1

            stmt_type = clean(row.get("stmt_type"))

            # Phase 1: consumption inventory = read/select events only
            if stmt_type != "SEL":
                continue

            selected_rows += 1

            log_timestamp = clean(row.get("log_timestamp"))
            user_name = clean(row.get("user_name"))
            consumer_app = clean(row.get("consumer_app"))
            app_type = clean(row.get("app_type"))
            domain = clean(row.get("domain"))
            database_name = clean(row.get("database_name"))
            table_name = clean(row.get("table_name"))
            zone = clean(row.get("zone"))

            qualified_name = f"{PLATFORM}.{database_name}.{table_name}"

            key = (
                consumer_app,
                app_type,
                domain,
                user_name,
                PLATFORM,
                database_name,
                table_name,
                qualified_name,
                zone,
                SOURCE,
            )

            state = aggregated[key]

            state["query_count"] += 1
            state["total_amp_cpu_sec"] += to_decimal(row.get("amp_cpu_sec"))
            state["total_elapsed_sec"] += to_decimal(row.get("elapsed_sec"))
            state["total_rows_read"] += to_int(row.get("row_count"))
            state["total_spool_gb"] += to_decimal(row.get("spool_gb"))

            if state["first_seen"] is None or log_timestamp < state["first_seen"]:
                state["first_seen"] = log_timestamp

            if state["last_seen"] is None or log_timestamp > state["last_seen"]:
                state["last_seen"] = log_timestamp

    fieldnames = [
        "batch_id",
        "source",
        "consumer_app",
        "app_type",
        "domain",
        "user_name",
        "platform",
        "database_name",
        "table_name",
        "qualified_name",
        "zone",
        "query_count",
        "first_seen",
        "last_seen",
        "total_amp_cpu_sec",
        "total_elapsed_sec",
        "total_rows_read",
        "total_spool_gb",
        "confidence",
    ]

    with output_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for key, state in sorted(aggregated.items()):
            (
                consumer_app,
                app_type,
                domain,
                user_name,
                platform,
                database_name,
                table_name,
                qualified_name,
                zone,
                source,
            ) = key

            writer.writerow(
                {
                    "batch_id": BATCH_ID,
                    "source": source,
                    "consumer_app": consumer_app,
                    "app_type": app_type,
                    "domain": domain,
                    "user_name": user_name,
                    "platform": platform,
                    "database_name": database_name,
                    "table_name": table_name,
                    "qualified_name": qualified_name,
                    "zone": zone,
                    "query_count": state["query_count"],
                    "first_seen": state["first_seen"],
                    "last_seen": state["last_seen"],
                    "total_amp_cpu_sec": str(state["total_amp_cpu_sec"]),
                    "total_elapsed_sec": str(state["total_elapsed_sec"]),
                    "total_rows_read": state["total_rows_read"],
                    "total_spool_gb": str(state["total_spool_gb"]),
                    "confidence": CONFIDENCE,
                }
            )

    print(f"Raw rows read: {raw_rows}")
    print(f"SEL rows used: {selected_rows}")
    print(f"Aggregated facts written: {len(aggregated)}")
    print(f"Output path: {output_path}")


if __name__ == "__main__":
    aggregate(RAW_INPUT_PATH, AGG_OUTPUT_PATH)