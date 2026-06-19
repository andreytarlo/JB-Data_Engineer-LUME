FROM python:3.13-slim

RUN pip install --no-cache-dir uv==0.5.11

WORKDIR /app

COPY pyproject.toml /app/pyproject.toml
RUN uv pip install --system --no-cache -r /app/pyproject.toml

COPY app /app/app

CMD ["python", "-m", "app.main"]
