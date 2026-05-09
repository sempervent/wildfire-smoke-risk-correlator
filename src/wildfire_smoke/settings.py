from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

import yaml
from dotenv import load_dotenv


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def load_yaml_config(name: str) -> dict:
    path = repo_root() / "config" / name
    if not path.exists():
        raise FileNotFoundError(f"Missing config file: {path}")
    return yaml.safe_load(path.read_text())


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap_servers: str

    postgres_host: str
    postgres_port: int
    postgres_db: str
    postgres_user: str
    postgres_password: str

    firms_map_key: str | None
    firms_source: str
    firms_bbox: str
    firms_day_range: str

    openaq_api_key: str | None
    openaq_bbox: str

    firms_dry_run: bool
    firms_fixture_csv: Path
    openaq_dry_run: bool
    openaq_fixture_jsonl: Path

    jdbc_url: str

    @staticmethod
    def from_env() -> "Settings":
        load_dotenv(repo_root() / ".env", override=False)

        sources = load_yaml_config("sources.yaml")

        kafka_bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")

        postgres_host = os.environ.get("POSTGRES_HOST", "localhost")
        postgres_port = int(os.environ.get("POSTGRES_PORT", "5432"))
        postgres_db = os.environ.get("POSTGRES_DB", "smoke")
        postgres_user = os.environ.get("POSTGRES_USER", "smoke")
        postgres_password = os.environ.get("POSTGRES_PASSWORD", "smoke")

        firms_map_key = os.environ.get("FIRMS_MAP_KEY") or None
        firms_source = os.environ.get("FIRMS_SOURCE", sources["firms"]["source_default"])
        firms_bbox = os.environ.get("FIRMS_BBOX", sources["firms"]["bbox_default"])
        firms_day_range = os.environ.get("FIRMS_DAY_RANGE", str(sources["firms"]["day_range_default"]))

        openaq_api_key = os.environ.get("OPENAQ_API_KEY") or None
        openaq_bbox = os.environ.get("OPENAQ_BBOX", sources["openaq"]["bbox_default"])

        firms_dry_run = os.environ.get("FIRMS_DRY_RUN", "0").strip().lower() in {"1", "true", "yes"}
        openaq_dry_run = os.environ.get("OPENAQ_DRY_RUN", "0").strip().lower() in {"1", "true", "yes"}

        firms_fixture_csv = Path(os.environ.get("FIRMS_FIXTURE_CSV", "tests/fixtures/firms_sample.csv"))
        openaq_fixture_jsonl = Path(os.environ.get("OPENAQ_FIXTURE_JSONL", "tests/fixtures/openaq_sample.jsonl"))

        jdbc_url = os.environ.get(
            "JDBC_URL",
            f"jdbc:postgresql://{postgres_host}:{postgres_port}/{postgres_db}",
        )

        return Settings(
            kafka_bootstrap_servers=kafka_bootstrap_servers,
            postgres_host=postgres_host,
            postgres_port=postgres_port,
            postgres_db=postgres_db,
            postgres_user=postgres_user,
            postgres_password=postgres_password,
            firms_map_key=firms_map_key,
            firms_source=firms_source,
            firms_bbox=firms_bbox,
            firms_day_range=firms_day_range,
            openaq_api_key=openaq_api_key,
            openaq_bbox=openaq_bbox,
            firms_dry_run=firms_dry_run,
            firms_fixture_csv=firms_fixture_csv,
            openaq_dry_run=openaq_dry_run,
            openaq_fixture_jsonl=openaq_fixture_jsonl,
            jdbc_url=jdbc_url,
        )


def kafka_topics() -> dict[str, str]:
    return load_yaml_config("sources.yaml")["kafka"]
