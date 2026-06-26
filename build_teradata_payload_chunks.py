import csv
import json
from pathlib import Path
from typing import Any


AGG_INPUT_PATH = Path("data/aggregated-teradata/consumption-aggregated.csv")
PAYLOAD_OUTPUT_DIR = Path("data/aggregated-payload")

PAYLOAD_TYPE = "consumer_inventory_facts"
SCHEMA_VERSION = "app_dataasset_consumption_v1"
SOURCE_SYSTEM = "Teradata DBQL"
CHUNK_SIZE = 50


def clean(value: str | None) -> str:
    return (value or "").strip()


def to_int(value: str | None) -> int:
    value = clean(value)
    if not value:
        return 0
    return int(float(value))


def to_float(value: str | None) -> float:
    value = clean(value)
    if not value:
        return 0.0
    return float(value)


def app_id(consumer_app: str) -> str:
    return "app_" + clean(consumer_app).lower().replace(" ", "_").replace("-", "_")


def asset_id(qualified_name: str) -> str:
    return "asset_" + clean(qualified_name).lower().replace(".", "_").replace("-", "_")


def build_fact(row: dict[str, str]) -> dict[str, Any]:
    consumer_app = clean(row.get("consumer_app"))
    qualified_name = clean(row.get("qualified_name"))

    return {
        "application": {
            "id": app_id(consumer_app),
            "name": consumer_app,
            "type": clean(row.get("app_type")),
            "domain": clean(row.get("domain")),
        },
        "data_asset": {
            "id": asset_id(qualified_name),
            "platform": clean(row.get("platform")),
            "database_name": clean(row.get("database_name")),
            "table_name": clean(row.get("table_name")),
            "qualified_name": qualified_name,
            "zone": clean(row.get("zone")),
        },
        "consumption": {
            "type": "CONSUMES",
            "source": clean(row.get("source")),
            "service_account": clean(row.get("user_name")),
            "query_count": to_int(row.get("query_count")),
            "first_seen": clean(row.get("first_seen")),
            "last_seen": clean(row.get("last_seen")),
            "total_amp_cpu_sec": to_float(row.get("total_amp_cpu_sec")),
            "total_elapsed_sec": to_float(row.get("total_elapsed_sec")),
            "total_rows_read": to_int(row.get("total_rows_read")),
            "total_spool_gb": to_float(row.get("total_spool_gb")),
            "confidence": to_float(row.get("confidence")),
        },
    }


def chunked(items: list[dict[str, Any]], chunk_size: int) -> list[list[dict[str, Any]]]:
    return [items[i : i + chunk_size] for i in range(0, len(items), chunk_size)]


def build_payload_chunks(input_path: str | Path, output_dir: str | Path) -> None:
    input_path = Path(input_path)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    facts: list[dict[str, Any]] = []

    with input_path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row in reader:
            facts.append(build_fact(row))

    chunks = chunked(facts, CHUNK_SIZE)

    for index, chunk in enumerate(chunks, start=1):
        chunk_id = f"td-dbql-poc-001-chunk-{index:03d}"

        payload = {
            "batch_id": "td-dbql-poc-001",
            "chunk_id": chunk_id,
            "payload_type": PAYLOAD_TYPE,
            "schema_version": SCHEMA_VERSION,
            "source_system": SOURCE_SYSTEM,
            "fact_count": len(chunk),
            "facts": chunk,
        }

        output_path = output_dir / f"teradata-consumption-chunk-{index:03d}.json"

        with output_path.open("w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)

    print(f"Aggregated facts read: {len(facts)}")
    print(f"Payload chunks written: {len(chunks)}")
    print(f"Output directory: {output_dir}")


if __name__ == "__main__":
    build_payload_chunks(AGG_INPUT_PATH, PAYLOAD_OUTPUT_DIR)