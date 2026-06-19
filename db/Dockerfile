FROM python:3.13-slim

RUN pip install --no-cache-dir uv==0.5.11

RUN uv pip install --system --no-cache \
    "psycopg[binary]>=3.1" \
    "pyarrow>=14.0"

COPY load_dimensions.py /app/load_dimensions.py

CMD ["python", "/app/load_dimensions.py"]
