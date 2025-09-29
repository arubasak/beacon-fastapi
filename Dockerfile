# --- Stage 1: Builder ---
# This stage installs dependencies, including any that need a compiler.
FROM python:3.11-slim as builder

# Set working directory
WORKDIR /app

# Install build-time system dependencies (like gcc for C extensions)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Create a virtual environment
ENV VIRTUAL_ENV=/app/venv
RUN python3 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

# Copy and install Python dependencies into the virtual environment
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt


# --- Stage 2: Final Image ---
# This stage creates the final, lean image for production.
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install only runtime system dependencies (curl for the healthcheck)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy the virtual environment from the builder stage
COPY --from=builder /app/venv /app/venv

# Copy the application code
COPY main.py .

# Create a non-root user and transfer ownership
RUN useradd --create-home --shell /bin/bash app \
    && chown -R app:app /app
USER app

# Activate the virtual environment by setting the PATH
ENV PATH="/app/venv/bin:$PATH"

# Expose the port the app runs on
EXPOSE 8000

# Health check to ensure the application is responsive
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Run the application using Gunicorn as a process manager for Uvicorn workers
# The number of workers (-w) can be tuned. 2-4 is a good start for a 1 vCPU instance.
CMD ["gunicorn", "-w", "2", "-k", "uvicorn.workers.UvicornWorker", "main:app", "--bind", "0.0.0.0:8000"]
