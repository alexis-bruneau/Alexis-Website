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
is web-only (no playwright); the scrape and Pages workflows do not use it.

`.github/workflows/deploy_azure.yml` rebuilds that image in Azure and updates the Container App on master pushes that touch the
web app (one-time setup, rollback, switching it off: [docs/AZURE_DEPLOY.md](docs/AZURE_DEPLOY.md)). **Never put the storage
connection string in the repo, a workflow or a log**: it lives only in the Container App secret `conn`.

## Branch workflow

- Work on feature branches and open a PR to **master**. Don't push to master directly, and don't merge PRs yourself.
- **master is production**: the scheduled `daily_scrape.yml` runs from master, and `deploy_pages.yml` publishes the site
  from master, and `deploy_azure.yml` deploys the web app from master. Mention in the PR when a change affects any of them.

## Engineering log (resume)

After completing any change that introduces a new tech component, an architectural or infrastructure decision, a non-trivial
algorithm, or measurable reliability/perf/cost/security work, add an entry to [docs/ENGINEERING_LOG.md](docs/ENGINEERING_LOG.md)
(format at the top of that file) in the same PR, and update the "Resume bullets" section if the change is significant. Skip
routine work (bug fixes, refactors, formatting, small UI tweaks). Never invent metrics: write `TBD`. When unsure, ask me at
the end of the task rather than adding noise.
