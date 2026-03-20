FROM python:3.14-slim

# Copy uv from the official image
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Set working directory
WORKDIR /app

# Enable bytecode compilation
ENV UV_COMPILE_BYTECODE=1

# Copy the dependency locks
COPY pyproject.toml uv.lock* ./

# Install dependencies into a virtual environment
RUN uv sync --frozen || uv sync

# Copy the rest of the application
COPY . .

# Place the virtual environment executables in PATH
ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONPATH="/app"

# Default command runs the manual test
CMD ["python", "manual_test.py"]
