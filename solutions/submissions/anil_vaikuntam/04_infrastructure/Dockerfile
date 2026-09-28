# =============================================================================
# Presight Data Engineering Assessment — Task 4.1
# Containerises the Task 2.2 ETL pipeline (etl_full.py)
# Author: Anil Vaikuntam
# =============================================================================
#
# Two-stage build:
#   1) builder — installs everything in requirements.txt into a venv. This
#      stage carries the full pip download/compile cache and build tooling;
#      none of that belongs in the image that actually runs.
#   2) runtime — copies ONLY the built venv + the solution/dataset code from
#      the builder stage onto a fresh python:3.11-slim base, so the final
#      image doesn't carry pip's cache, apt lists, or any build-time layer.

# ---- Stage 1: builder --------------------------------------------------------
FROM python:3.11-slim AS builder

WORKDIR /build

COPY requirements.txt .
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# ---- Stage 2: runtime ---------------------------------------------------------
FROM python:3.11-slim AS runtime

# Copy the pre-built venv from the builder stage — this is the entire reason
# for the multi-stage split: the builder's apt/pip build layers never reach
# this image, only the resulting installed packages do.
COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

WORKDIR /app

# Solution code + the datasets it reads. Directory structure is preserved
# exactly as in the repo (solutions/submissions/anil_vaikuntam/...) so
# etl_full.py's own path-resolution logic (BASE_DIR computed relative to its
# own file location) resolves correctly with no code changes for "running in
# a container" vs. "running on the host".
COPY solutions/ ./solutions/
COPY datasets/ ./datasets/

# Task 4.1 requirement: pipeline reads DATA_DIR/OUTPUT_DIR from the
# environment, with sensible defaults (etl_pipeline.py / etl_full.py both
# fall back to BASE_DIR-relative paths — /app/datasets, /app/outputs — if
# these aren't set, so the container still runs correctly without them; set
# explicitly here for clarity and to match docker-compose.override.yml).
ENV DATA_DIR=/app/datasets
ENV OUTPUT_DIR=/app/outputs

# Output volume — mount ./outputs here at `docker run` time so results land
# on the host: docker run -v $(pwd)/outputs:/app/outputs presight-etl
VOLUME ["/app/outputs"]

ENTRYPOINT ["python", "solutions/submissions/anil_vaikuntam/02_sql_and_viz/etl_full.py"]
