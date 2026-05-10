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

    wind_dry_run: bool
    wind_fixture_jsonl: Path
    wind_source: str
    wind_bbox: str | None
    wind_station_ids: tuple[str, ...]
    wind_match_radius_km: float
    wind_match_lookback_hours: float
    plume_max_distance_km: float
    plume_half_angle_degrees: float

    grid_weather_enabled: bool
    grid_weather_source: str
    grid_weather_dry_run: bool
    grid_weather_fixture_json: Path
    grid_weather_bbox: str | None
    grid_weather_max_points: int
    grid_weather_lookahead_hours: int
    grid_weather_variables: tuple[str, ...]
    grid_weather_cell_size_degrees: float
    grid_weather_refuse_large_bbox: bool

    fire_weather_match_radius_km: float
    fire_weather_match_max_time_delta_hours: float
    fire_weather_match_method: str

    plume_model_version: str
    plume_grid_fallback_to_station: bool

    jdbc_url: str

    smoke_risk_model_version: str
    smoke_risk_lookback_hours: int
    smoke_risk_nearby_km: float
    smoke_risk_geographies: str

    fixture_time_mode: str
    fixture_relative_base_hours_ago: float

    grid_weather_points_lonlat: tuple[tuple[float, float], ...]

    dispersion_enabled: bool
    dispersion_model_version: str
    dispersion_max_distance_km: float
    dispersion_crosswind_sigma_km: float
    dispersion_downwind_sigma_km: float
    dispersion_min_wind_speed_mps: float
    dispersion_source_strength_mode: str
    dispersion_grid_resolution_km: float
    dispersion_max_target_geographies: int
    dispersion_lookback_hours: int
    dispersion_use_grid_weather: bool
    dispersion_fallback_to_station_wind: bool
    dispersion_write_debug_fields: bool
    dispersion_allow_large_run: bool

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
        wind_dry_run = os.environ.get("WIND_DRY_RUN", "0").strip().lower() in {"1", "true", "yes"}

        firms_fixture_csv = Path(os.environ.get("FIRMS_FIXTURE_CSV", "tests/fixtures/firms_sample.csv"))
        openaq_fixture_jsonl = Path(os.environ.get("OPENAQ_FIXTURE_JSONL", "tests/fixtures/openaq_sample.jsonl"))
        wind_fixture_jsonl = Path(os.environ.get("WIND_FIXTURE_JSONL", "tests/fixtures/wind_sample.jsonl"))

        wind_source = os.environ.get("WIND_SOURCE", "nws").strip().lower()
        wind_bbox_raw = os.environ.get("WIND_BBOX", "").strip()
        wind_bbox = wind_bbox_raw if wind_bbox_raw else None
        station_raw = os.environ.get("WIND_STATION_IDS", os.environ.get("WIND_STATION_ID", "")).strip()
        wind_station_ids = tuple(s.strip().upper() for s in station_raw.split(",") if s.strip())

        wind_match_radius_km = float(os.environ.get("WIND_MATCH_RADIUS_KM", "100"))
        wind_match_lookback_hours = float(os.environ.get("WIND_MATCH_LOOKBACK_HOURS", "6"))
        plume_max_distance_km = float(os.environ.get("PLUME_MAX_DISTANCE_KM", "150"))
        plume_half_angle_degrees = float(os.environ.get("PLUME_HALF_ANGLE_DEGREES", "30"))

        gw_yaml = load_yaml_config("sources.yaml").get("grid_weather") or {}
        grid_weather_enabled = os.environ.get("GRID_WEATHER_ENABLED", "0").strip().lower() in {"1", "true", "yes"}
        grid_weather_source = os.environ.get("GRID_WEATHER_SOURCE", "nws_gridpoint").strip()
        grid_weather_dry_run = os.environ.get("GRID_WEATHER_DRY_RUN", "1").strip().lower() in {"1", "true", "yes"}
        grid_weather_fixture_json = Path(
            os.environ.get("GRID_WEATHER_FIXTURE_JSON", "tests/fixtures/nws_gridpoint_sample.json")
        )
        grid_bbox_env = os.environ.get("GRID_WEATHER_BBOX", "").strip()
        grid_weather_bbox = grid_bbox_env or str(gw_yaml.get("bbox_default") or "").strip() or None
        grid_weather_max_points = int(os.environ.get("GRID_WEATHER_MAX_POINTS", str(gw_yaml.get("max_points_default") or 100)))
        if grid_weather_max_points < 1:
            raise ValueError("GRID_WEATHER_MAX_POINTS must be >= 1")
        grid_weather_lookahead_hours = int(
            os.environ.get("GRID_WEATHER_LOOKAHEAD_HOURS", str(gw_yaml.get("lookahead_hours_default") or 24))
        )
        gv_raw = os.environ.get("GRID_WEATHER_VARIABLES", "windSpeed,windDirection,temperature,relativeHumidity")
        grid_weather_variables = tuple(s.strip() for s in gv_raw.split(",") if s.strip())
        grid_weather_cell_size_degrees = float(
            os.environ.get(
                "GRID_WEATHER_CELL_SIZE_DEGREES",
                str(gw_yaml.get("cell_size_degrees_default") or 0.05),
            )
        )
        if grid_weather_cell_size_degrees <= 0:
            raise ValueError("GRID_WEATHER_CELL_SIZE_DEGREES must be > 0")
        grid_weather_refuse_large_bbox = os.environ.get("GRID_WEATHER_REFUSE_LARGE_BBOX", "1").strip().lower() in {
            "1",
            "true",
            "yes",
        }

        fire_weather_match_radius_km = float(os.environ.get("FIRE_WEATHER_MATCH_RADIUS_KM", "50"))
        fire_weather_match_max_time_delta_hours = float(os.environ.get("FIRE_WEATHER_MATCH_MAX_TIME_DELTA_HOURS", "3"))
        fire_weather_match_method = os.environ.get("FIRE_WEATHER_MATCH_METHOD", "nearest_grid_cell").strip()

        plume_model_version = os.environ.get("PLUME_MODEL_VERSION", "wind_v1").strip().lower()
        if plume_model_version not in {"wind_v1", "wind_grid_v2"}:
            raise ValueError("PLUME_MODEL_VERSION must be one of: wind_v1, wind_grid_v2")
        plume_grid_fallback_to_station = os.environ.get("PLUME_GRID_FALLBACK_TO_STATION", "1").strip().lower() in {
            "1",
            "true",
            "yes",
        }

        jdbc_url = os.environ.get(
            "JDBC_URL",
            f"jdbc:postgresql://{postgres_host}:{postgres_port}/{postgres_db}",
        )

        smoke_risk_model_version = (
            os.environ.get("RISK_MODEL_VERSION") or os.environ.get("SMOKE_RISK_MODEL_VERSION", "v2")
        ).strip().lower()
        if smoke_risk_model_version not in {"v1", "v2", "v3", "v4", "v5"}:
            raise ValueError("SMOKE_RISK_MODEL_VERSION must be one of: v1, v2, v3, v4, v5")

        smoke_risk_lookback_hours = int(os.environ.get("SMOKE_RISK_LOOKBACK_HOURS", "24"))
        if smoke_risk_lookback_hours < 1:
            raise ValueError("SMOKE_RISK_LOOKBACK_HOURS must be >= 1")

        smoke_risk_nearby_km = float(os.environ.get("SMOKE_RISK_NEARBY_KM", "50"))
        if smoke_risk_nearby_km <= 0:
            raise ValueError("SMOKE_RISK_NEARBY_KM must be > 0")

        smoke_risk_geographies = os.environ.get("SMOKE_RISK_GEOGRAPHIES", "both").strip().lower()
        if smoke_risk_geographies not in {"county", "tract", "both"}:
            raise ValueError("SMOKE_RISK_GEOGRAPHIES must be one of: county, tract, both")

        fixture_time_mode = os.environ.get("FIXTURE_TIME_MODE", "static").strip().lower()
        if fixture_time_mode not in {"static", "relative"}:
            raise ValueError("FIXTURE_TIME_MODE must be one of: static, relative")
        fixture_relative_base_hours_ago = float(os.environ.get("FIXTURE_RELATIVE_BASE_HOURS_AGO", "1"))

        pts: list[tuple[float, float]] = []
        gw_pts_raw = os.environ.get("GRID_WEATHER_POINTS", "").strip()
        for chunk in gw_pts_raw.split(";"):
            chunk = chunk.strip()
            if not chunk:
                continue
            parts = chunk.split(",")
            if len(parts) != 2:
                raise ValueError("GRID_WEATHER_POINTS entries must be lon,lat separated by ';'")
            pts.append((float(parts[0].strip()), float(parts[1].strip())))
        grid_weather_points_lonlat = tuple(pts)

        dispersion_enabled = os.environ.get("DISPERSION_ENABLED", "0").strip().lower() in {"1", "true", "yes"}
        dispersion_model_version = os.environ.get("DISPERSION_MODEL_VERSION", "gaussian_v0").strip()
        dispersion_max_distance_km = float(os.environ.get("DISPERSION_MAX_DISTANCE_KM", "150"))
        dispersion_crosswind_sigma_km = float(os.environ.get("DISPERSION_CROSSWIND_SIGMA_KM", "15"))
        dispersion_downwind_sigma_km = float(os.environ.get("DISPERSION_DOWNWIND_SIGMA_KM", "75"))
        dispersion_min_wind_speed_mps = float(os.environ.get("DISPERSION_MIN_WIND_SPEED_MPS", "0.5"))
        dispersion_source_strength_mode = os.environ.get("DISPERSION_SOURCE_STRENGTH_MODE", "frp").strip().lower()
        dispersion_grid_resolution_km = float(os.environ.get("DISPERSION_GRID_RESOLUTION_KM", "10"))
        dispersion_max_target_geographies = int(os.environ.get("DISPERSION_MAX_TARGET_GEOGRAPHIES", "500"))
        if dispersion_max_target_geographies < 1:
            raise ValueError("DISPERSION_MAX_TARGET_GEOGRAPHIES must be >= 1")
        dispersion_lookback_hours = int(os.environ.get("DISPERSION_LOOKBACK_HOURS", "24"))
        if dispersion_lookback_hours < 1:
            raise ValueError("DISPERSION_LOOKBACK_HOURS must be >= 1")
        dispersion_use_grid_weather = os.environ.get("DISPERSION_USE_GRID_WEATHER", "1").strip().lower() in {
            "1",
            "true",
            "yes",
        }
        dispersion_fallback_to_station_wind = os.environ.get(
            "DISPERSION_FALLBACK_TO_STATION_WIND", "1"
        ).strip().lower() in {"1", "true", "yes"}
        dispersion_write_debug_fields = os.environ.get("DISPERSION_WRITE_DEBUG_FIELDS", "1").strip().lower() in {
            "1",
            "true",
            "yes",
        }
        dispersion_allow_large_run = os.environ.get("DISPERSION_ALLOW_LARGE_RUN", "0").strip().lower() in {
            "1",
            "true",
            "yes",
        }

        if dispersion_source_strength_mode not in {"frp", "brightness", "unit"}:
            raise ValueError("DISPERSION_SOURCE_STRENGTH_MODE must be one of: frp, brightness, unit")

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
            wind_dry_run=wind_dry_run,
            wind_fixture_jsonl=wind_fixture_jsonl,
            wind_source=wind_source,
            wind_bbox=wind_bbox,
            wind_station_ids=wind_station_ids,
            wind_match_radius_km=wind_match_radius_km,
            wind_match_lookback_hours=wind_match_lookback_hours,
            plume_max_distance_km=plume_max_distance_km,
            plume_half_angle_degrees=plume_half_angle_degrees,
            grid_weather_enabled=grid_weather_enabled,
            grid_weather_source=grid_weather_source,
            grid_weather_dry_run=grid_weather_dry_run,
            grid_weather_fixture_json=grid_weather_fixture_json,
            grid_weather_bbox=grid_weather_bbox,
            grid_weather_max_points=grid_weather_max_points,
            grid_weather_lookahead_hours=grid_weather_lookahead_hours,
            grid_weather_variables=grid_weather_variables,
            grid_weather_cell_size_degrees=grid_weather_cell_size_degrees,
            grid_weather_refuse_large_bbox=grid_weather_refuse_large_bbox,
            fire_weather_match_radius_km=fire_weather_match_radius_km,
            fire_weather_match_max_time_delta_hours=fire_weather_match_max_time_delta_hours,
            fire_weather_match_method=fire_weather_match_method,
            plume_model_version=plume_model_version,
            plume_grid_fallback_to_station=plume_grid_fallback_to_station,
            jdbc_url=jdbc_url,
            smoke_risk_model_version=smoke_risk_model_version,
            smoke_risk_lookback_hours=smoke_risk_lookback_hours,
            smoke_risk_nearby_km=smoke_risk_nearby_km,
            smoke_risk_geographies=smoke_risk_geographies,
            fixture_time_mode=fixture_time_mode,
            fixture_relative_base_hours_ago=fixture_relative_base_hours_ago,
            grid_weather_points_lonlat=grid_weather_points_lonlat,
            dispersion_enabled=dispersion_enabled,
            dispersion_model_version=dispersion_model_version,
            dispersion_max_distance_km=dispersion_max_distance_km,
            dispersion_crosswind_sigma_km=dispersion_crosswind_sigma_km,
            dispersion_downwind_sigma_km=dispersion_downwind_sigma_km,
            dispersion_min_wind_speed_mps=dispersion_min_wind_speed_mps,
            dispersion_source_strength_mode=dispersion_source_strength_mode,
            dispersion_grid_resolution_km=dispersion_grid_resolution_km,
            dispersion_max_target_geographies=dispersion_max_target_geographies,
            dispersion_lookback_hours=dispersion_lookback_hours,
            dispersion_use_grid_weather=dispersion_use_grid_weather,
            dispersion_fallback_to_station_wind=dispersion_fallback_to_station_wind,
            dispersion_write_debug_fields=dispersion_write_debug_fields,
            dispersion_allow_large_run=dispersion_allow_large_run,
        )


def kafka_topics() -> dict[str, str]:
    return load_yaml_config("sources.yaml")["kafka"]
