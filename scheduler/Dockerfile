FROM python:3.13-slim
RUN pip install --no-cache-dir uv==0.5.11
RUN uv pip install --system --no-cache \
    "psycopg[binary]>=3.1" \
    "apscheduler>=3.10"
COPY app/ /app/
CMD ["python", "/app/main.py"]
