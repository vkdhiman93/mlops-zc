FROM agrigorev/zoomcamp-model:mlops-2024-3.10.13-slim

WORKDIR /app

# Install uv (minimal dependency manager)
RUN pip install uv

# Copy project metadata and lock file
COPY pyproject.toml uv.lock ./

# Sync all deps into the global Python (no virtualenv in Docker)
RUN uv sync

# Copy source code
COPY starter.py ./

# Set default entrypoint
CMD ["uv", "run", "starter.py", "--year=2023", "--month=5"]
