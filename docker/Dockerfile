# Start from a python 3.11 image and add poetry  and psql libraries
FROM python:3.12.8-bullseye

RUN set -eux; \
apt-get update; \
apt-get install -y --no-install-recommends \
    libpq-dev pkg-config gcc build-essential\
; \
apt-get clean; \
rm -rf /var/lib/apt/lists/*

# RUN curl -sSL https://install.python-poetry.org | python3 -
RUN pip install poetry

# Poetry's configuration:
# ENV PATH="/root/.local/bin:${PATH}"

# Create home and set it to cwd
RUN useradd --create-home --shell /bin/bash emm
WORKDIR /home/emm

# Install dependencies
# COPY . /home/emm
COPY poetry.lock poetry.lock
COPY pyproject.toml pyproject.toml
RUN ls /home/emm
RUN poetry install
RUN rm -rf poetry.lock pyproject.toml

# Use bash as CMD so that we can make it interactive
CMD ["bash"]
