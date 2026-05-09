FROM apache/spark:3.5.4-java17-python3

USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends python3-pip \
  && python3 -m pip install --no-cache-dir \
    "httpx>=0.27" \
    "kafka-python-ng>=2.2" \
    "psycopg[binary]>=3.2" \
    "python-dotenv>=1.0" \
    "pyyaml>=6.0" \
  && rm -rf /var/lib/apt/lists/*

# Spark's Ivy resolver writes here when using --packages; ensure it exists for the non-root runtime user.
RUN mkdir -p /home/spark/.ivy2/cache \
  && chown -R spark:spark /home/spark/.ivy2

USER spark
