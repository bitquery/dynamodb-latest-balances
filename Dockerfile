FROM python:3.11-slim AS builder
WORKDIR /app
COPY requirements.txt .
RUN pip install --user --no-cache-dir -r requirements.txt

FROM python:3.11-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /root/.local /root/.local
COPY proto_compiled/ ./proto_compiled/
COPY src/ ./src/
COPY config/ ./config/
COPY local/ ./local/
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
ENV PATH=/root/.local/bin:$PATH
ENV PYTHONUNBUFFERED=1
HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 CMD python -c "import sys; sys.exit(0)" || exit 1
CMD ["python", "src/main.py"]
