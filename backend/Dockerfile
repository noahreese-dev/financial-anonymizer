# ============================================================================
# Presidio Deep Clean Service - Isolated Container
# ============================================================================
# Security guarantees:
# - No volume mounts (can't access host filesystem)
# - No persistent storage
# - Runs as non-root user
# - Localhost networking only (configured in docker-compose)
# ============================================================================

FROM python:3.11-slim

# Security: Don't run as root
RUN useradd --create-home --shell /bin/bash presidio
WORKDIR /app

# Install system dependencies for spaCy
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first (better caching)
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Download spaCy English model
RUN python -m spacy download en_core_web_md

# Copy application code
COPY main.py .

# Switch to non-root user
USER presidio

# Expose port (internal to Docker network)
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health')" || exit 1

# Run the API
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]

