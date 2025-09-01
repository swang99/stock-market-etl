# Step 1: Base image with Python
FROM python:3.9-slim

# Step 2: Set working directory inside the container
WORKDIR /app

# Step 3: Install system dependencies (needed for psycopg2, pyarrow, etc.)
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Step 4: Copy requirements first (for caching)
COPY requirements.txt .

# Step 5: Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Step 6: Copy your code
COPY . .

# Step 7: Default command (can be overridden at runtime)
CMD ["python", "scripts/run_pipeline.py"]
