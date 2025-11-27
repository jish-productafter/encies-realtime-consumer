FROM python:3.12-slim

WORKDIR /app

# Install uv for faster dependency management
RUN pip install --no-cache-dir uv

# Copy dependency files first for better caching
COPY pyproject.toml uv.lock* ./

# Install dependencies using uv
# This will install all dependencies from pyproject.toml
RUN uv pip install --system .

# Copy application code
COPY . .

# Expose the port
EXPOSE 8000

# Run the application
CMD ["python", "main.py"]

