FROM python:3.11-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    MISE_GITHUB_ATTESTATIONS=false \
    MISE_GITHUB_GITHUB_ATTESTATIONS=false \
    MISE_AQUA_GITHUB_ATTESTATIONS=false

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc g++ make curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/requirements.txt
RUN python -m pip install --upgrade pip setuptools wheel \
    && python -m pip install -r /app/requirements.txt

COPY . /app

CMD ["python", "main.py"]
