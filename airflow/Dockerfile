FROM apache/airflow:2.5.1

# Install browser system deps as root (what `playwright install --with-deps`
# would do on Debian Bullseye, which apache/airflow:2.5.1 is based on).
# Doing it ahead of pip install so the layer caches independently of
# requirements.txt churn.
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
        libnss3 \
        libnspr4 \
        libatk1.0-0 \
        libatk-bridge2.0-0 \
        libcups2 \
        libdrm2 \
        libxkbcommon0 \
        libxcomposite1 \
        libxdamage1 \
        libxfixes3 \
        libxrandr2 \
        libgbm1 \
        libxcb1 \
        libxshmfence1 \
        libpango-1.0-0 \
        libcairo2 \
        libasound2 \
 && rm -rf /var/lib/apt/lists/*
USER airflow

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# Chromium binary only — apt deps are already installed above. Runs as the
# airflow user (matches where pip put playwright's site-packages).
RUN playwright install chromium
