from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta, timezone

from kafka import KafkaProducer
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType

from wildfire_smoke.db.connection import connect, jdbc_properties, jdbc_url
from wildfire_smoke.risk import compute_risk_score_fields
from wildfire_smoke.settings import kafka_topics

log = logging.getLogger(__name__)


def main() -> None:
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
    topics = kafka_topics()

    window_end = datetime.now(timezone.utc).replace(microsecond=0)
    window_start = (window_end - timedelta(hours=24)).replace(microsecond=0)

    ws = window_start.strftime("%Y-%m-%d %H:%M:%S+00")
    we = window_end.strftime("%Y-%m-%d %H:%M:%S+00")

    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                DELETE FROM analytics.smoke_risk_scores
                WHERE window_start = %s AND window_end = %s
                """,
                (window_start, window_end),
            )
        conn.commit()

    county_fires_sql = f"""
        SELECT county_geoid AS geoid,
               COUNT(*)::int AS fire_count,
               MAX(frp) AS max_frp
        FROM normalized.fire_detections
        WHERE acq_datetime >= '{ws}'::timestamptz
          AND acq_datetime < '{we}'::timestamptz
          AND county_geoid IS NOT NULL
        GROUP BY county_geoid
        """

    county_aq_sql = f"""
        SELECT county_geoid AS geoid,
               AVG(value) FILTER (WHERE parameter = 'pm25') AS avg_pm25,
               AVG(value) FILTER (WHERE parameter = 'pm10') AS avg_pm10
        FROM normalized.air_quality_measurements
        WHERE measured_at >= '{ws}'::timestamptz
          AND measured_at < '{we}'::timestamptz
          AND county_geoid IS NOT NULL
        GROUP BY county_geoid
        """

    tract_fires_sql = f"""
        SELECT tract_geoid AS geoid,
               COUNT(*)::int AS fire_count,
               MAX(frp) AS max_frp
        FROM normalized.fire_detections
        WHERE acq_datetime >= '{ws}'::timestamptz
          AND acq_datetime < '{we}'::timestamptz
          AND tract_geoid IS NOT NULL
        GROUP BY tract_geoid
        """

    tract_aq_sql = f"""
        SELECT tract_geoid AS geoid,
               AVG(value) FILTER (WHERE parameter = 'pm25') AS avg_pm25,
               AVG(value) FILTER (WHERE parameter = 'pm10') AS avg_pm10
        FROM normalized.air_quality_measurements
        WHERE measured_at >= '{ws}'::timestamptz
          AND measured_at < '{we}'::timestamptz
          AND tract_geoid IS NOT NULL
        GROUP BY tract_geoid
        """

    spark = SparkSession.builder.appName("compute-smoke-risk").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    url = jdbc_url()
    props = jdbc_properties()

    fires_c = spark.read.jdbc(url, f"({county_fires_sql}) t", properties=props)
    aq_c = spark.read.jdbc(url, f"({county_aq_sql}) t", properties=props)
    county = (
        fires_c.join(aq_c, on="geoid", how="outer")
        .fillna(0, subset=["fire_count", "max_frp", "avg_pm25", "avg_pm10"])
        .withColumn("geography_type", F.lit("county"))
    )

    fires_t = spark.read.jdbc(url, f"({tract_fires_sql}) t", properties=props)
    aq_t = spark.read.jdbc(url, f"({tract_aq_sql}) t", properties=props)
    tract = (
        fires_t.join(aq_t, on="geoid", how="outer")
        .fillna(0, subset=["fire_count", "max_frp", "avg_pm25", "avg_pm10"])
        .withColumn("geography_type", F.lit("tract"))
    )

    combined = county.unionByName(tract)

    risk_score_udf = F.udf(
        lambda fc, frp, p25, p10: compute_risk_score_fields(
            int(fc or 0),
            float(frp) if frp is not None else None,
            float(p25) if p25 is not None else None,
            float(p10) if p10 is not None else None,
        )[0],
        DoubleType(),
    )
    risk_band_udf = F.udf(
        lambda fc, frp, p25, p10: compute_risk_score_fields(
            int(fc or 0),
            float(frp) if frp is not None else None,
            float(p25) if p25 is not None else None,
            float(p10) if p10 is not None else None,
        )[1],
        StringType(),
    )

    scored = combined.withColumn("risk_score", risk_score_udf("fire_count", "max_frp", "avg_pm25", "avg_pm10")).withColumn(
        "risk_band",
        risk_band_udf("fire_count", "max_frp", "avg_pm25", "avg_pm10"),
    )

    rows = scored.collect()

    kafka_servers = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
    producer = KafkaProducer(
        bootstrap_servers=[s.strip() for s in kafka_servers.split(",") if s.strip()],
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
    )

    upsert_sql = """
        INSERT INTO analytics.smoke_risk_scores (
          geography_type, geoid, window_start, window_end,
          fire_count, max_frp, avg_pm25, avg_pm10, risk_score, risk_band
        ) VALUES (
          %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (geography_type, geoid, window_start, window_end)
        DO UPDATE SET
          fire_count = EXCLUDED.fire_count,
          max_frp = EXCLUDED.max_frp,
          avg_pm25 = EXCLUDED.avg_pm25,
          avg_pm10 = EXCLUDED.avg_pm10,
          risk_score = EXCLUDED.risk_score,
          risk_band = EXCLUDED.risk_band,
          computed_at = now();
        """

    with connect() as conn:
        with conn.cursor() as cur:
            for r in rows:
                cur.execute(
                    upsert_sql,
                    (
                        r["geography_type"],
                        r["geoid"],
                        window_start,
                        window_end,
                        int(r["fire_count"] or 0),
                        float(r["max_frp"]) if r["max_frp"] is not None else None,
                        float(r["avg_pm25"]) if r["avg_pm25"] is not None else None,
                        float(r["avg_pm10"]) if r["avg_pm10"] is not None else None,
                        float(r["risk_score"]),
                        str(r["risk_band"]),
                    ),
                )

                payload = {
                    "geography_type": r["geography_type"],
                    "geoid": r["geoid"],
                    "window_start": window_start.isoformat(),
                    "window_end": window_end.isoformat(),
                    "fire_count": int(r["fire_count"] or 0),
                    "max_frp": float(r["max_frp"]) if r["max_frp"] is not None else None,
                    "avg_pm25": float(r["avg_pm25"]) if r["avg_pm25"] is not None else None,
                    "avg_pm10": float(r["avg_pm10"]) if r["avg_pm10"] is not None else None,
                    "risk_score": float(r["risk_score"]),
                    "risk_band": str(r["risk_band"]),
                }
                producer.send(topics["smoke_risk_topic"], value=payload)

        conn.commit()

    producer.flush()
    producer.close()

    spark.stop()
    log.info(
        "smoke_risk_complete",
        extra={
            "window_start": window_start.isoformat(),
            "window_end": window_end.isoformat(),
            "rows": len(rows),
        },
    )


if __name__ == "__main__":
    main()
