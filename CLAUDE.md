# Alexis-Website

Two things live in this repo:

- **Portfolio website**: `templates/`, `static/`, `build_site.py`. Published to GitHub Pages by
  `.github/workflows/deploy_pages.yml` on every push to master (static build; the Redfin pages are excluded).
- **Redfin pipeline**: `redfin/` (Python package `redfin`) plus its Flask routes in `app/main.py` (`/ottawa_map`,
  `/ottawa_active`). **Read [redfin/CLAUDE.md](redfin/CLAUDE.md) before working on anything Redfin.**

## Setup and tests

```bash
pip install -r requirements.txt       # Flask app
pip install -e "./redfin[dev]"        # Redfin package + pytest
python -m pytest redfin               # all tests
python app/main.py                    # local Flask app (needs AZURE_STORAGE_CONNECTION_STRING in .env)
```

The web app also builds as a Docker image (`Dockerfile`, `.dockerignore`; guide: [docs/DOCKER.md](docs/DOCKER.md)):
`docker build -t alexis-website .` then `docker run --rm -p 8000:8000 --env-file .env alexis-website`. `requirements.txt`
is web-only (no playwright); the workflows do not use it.

## Branch workflow

- Work on feature branches and open a PR to **master**. Don't push to master directly, and don't merge PRs yourself.
- **master is production**: the scheduled `daily_scrape.yml` runs from master, and `deploy_pages.yml` publishes the site
  from master. Mention in the PR when a change affects either one.
