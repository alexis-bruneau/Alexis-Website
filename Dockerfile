# Web app image: the portfolio + the Redfin map (app/main.py), served by gunicorn.
# Not in the image: playwright / the scraper's packages, .env, cookies, tests (see .dockerignore).
# Guide for beginners: docs/DOCKER.md

FROM python:3.10-slim

# PYTHONDONTWRITEBYTECODE: no .pyc files in the image.  PYTHONUNBUFFERED: print() shows in `docker logs` at once.
# PIP_NO_CACHE_DIR: pip's download cache would only make the layer bigger.
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# 1) Dependencies first. This layer is rebuilt only when requirements.txt changes, so editing code never
#    re-downloads pandas, pyarrow, duckdb and the rest.
COPY requirements.txt .
RUN pip install -r requirements.txt

# 2) Our code. Changes often, so it comes after the slow layer above.
#    The folder layout is kept as in the repo: redfin/config.py finds redfin/data/ relative to its own file.
COPY app ./app
COPY templates ./templates
COPY static ./static
COPY redfin ./redfin

# 3) The redfin package. --no-deps: its pyproject also lists playwright, beautifulsoup4 and requests (needed to
#    scrape, not to serve), and everything the web app needs is already installed by step 1.
#    -e (editable) keeps the package in /app/redfin/src, next to redfin/data/neighbourhoods/ that config.py reads.
#    A normal install would copy it into site-packages and that data file would not be found.
RUN pip install --no-deps -e ./redfin

# Run as a normal user, not root: a bug in the app then cannot change the container's system files.
RUN useradd --create-home --uid 1000 appuser
USER appuser

# Documentation only (does not publish the port; `docker run -p` does that). Azure sets PORT at run time.
ENV PORT=8000
EXPOSE 8000

# 1 worker + 4 threads: app/main.py downloads the data from Azure when the module is imported, so each worker
# process holds its own full copy in memory. Threads share one copy; /refresh-data then reloads the only copy.
# --timeout 120: importing the app (the Azure download) happens inside the worker, and the default 30 s would
# kill a slow start. `exec` makes gunicorn the main process, so `docker stop` reaches it.
# Shell form on purpose: ${PORT:-8000} is expanded by sh when the container starts.
CMD ["sh", "-c", "exec gunicorn app.main:app --bind 0.0.0.0:${PORT:-8000} --workers 1 --worker-class gthread --threads 4 --timeout 120 --access-logfile - --error-logfile -"]
