# Start from a python 3.11 image and add poetry  and psql libraries
FROM python:3.11.9-slim-bullseye

RUN set -eux; \
apt-get update; \
apt-get install -y --no-install-recommends \
    libpq-dev \
; \
rm -rf /var/lib/apt/lists/*

RUN curl -sSL https://install.python-poetry.org | python3 -
