FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Create non-root user for security
RUN adduser --disabled-password --gecos '' appuser && \
    chown -R appuser:appuser /app

# Install dependencies first (leverage Docker layer caching)
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy application source code
COPY src/ ./src/

# Create data directory for SQLite persistence (owned by appuser)
RUN mkdir -p /app/data && chown -R appuser:appuser /app/data

# Switch to non-root user
USER appuser

# Expose the application port
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8080/health')" || exit 1

# Run the application
CMD ["python", "-m", "src.main"]
