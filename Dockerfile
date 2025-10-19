FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Install system deps (add any you need later)
RUN apt-get update -y && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy project files
COPY pyproject.toml ./
COPY src ./src

# Install package
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir .

# Default to a harmless command; jobs override with --command/--args
ENTRYPOINT ["python"]
CMD ["-c", "print('ai-sportsbettor image ready')"]


