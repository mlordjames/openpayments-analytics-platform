FROM python:3.11-slim

WORKDIR /app

# Install uv (fast pip replacement)
RUN pip install --no-cache-dir uv

# Install deps (copy first for better Docker cache)
COPY docker/requirements.txt /app/requirements.txt
RUN uv pip install --system -r /app/requirements.txt

# Copy project code needed at runtime
COPY scripts/ /app/scripts/
COPY run_pipeline.py /app/run_pipeline.py

# Default entrypoint now runs the pipeline runner
ENTRYPOINT ["python", "/app/run_pipeline.py"]
CMD ["--help"]
