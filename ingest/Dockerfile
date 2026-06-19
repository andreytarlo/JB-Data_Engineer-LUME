FROM python:3.13-slim

RUN pip install --no-cache-dir uv==0.5.11

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

# Install dependencies first (cacheable layer).
COPY pyproject.toml /app/pyproject.toml
RUN uv pip install --system --no-cache -r /app/pyproject.toml

# Then the application code.
COPY app /app/app

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"]
