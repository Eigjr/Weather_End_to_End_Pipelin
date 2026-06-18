FROM python:3.10-slim

WORKDIR /transformation

RUN apt-get update && apt-get install -y \
    git \
    gcc \
    g++ \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# DO NOT force COPY full project in Docker (important for your setup)
# Your project should be mounted via docker-compose

CMD ["dbt", "--version"]