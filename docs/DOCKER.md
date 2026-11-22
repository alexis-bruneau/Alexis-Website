# Docker guide for this project

Written for someone new to Docker. It explains the `Dockerfile` and `.dockerignore` in this repo line by line, then lists the
commands you will use every day.

## The five words you need

| Word | What it is | Analogy |
|---|---|---|
| **Image** | A frozen, read-only package: Linux files + Python + your code + the command to start. Built once. | A recipe-and-ingredients box, sealed |
| **Container** | A running copy of an image. Start many from one image; delete them freely. | A meal cooked from the box |
| **Layer** | An image is a stack of layers, one per Dockerfile instruction. Docker caches each. | Saved game checkpoints |
| **Build context** | The folder you build from (`.`). Everything in it is sent to Docker, except `.dockerignore` entries. | The box you hand to the cook |
| **Docker Desktop** | The Windows app that runs the Docker engine (the "daemon"). It must be open or every `docker` command fails with "failed to connect to the docker API". | The kitchen |

## What this image does

It runs `app/main.py` (the portfolio and the Redfin maps) with **gunicorn**, a real web server. Flask's own `app.run()` is only for
development. The image holds the code and the Python packages. It does **not** hold your Azure connection string or the data: the
app downloads the data from Azure when it starts.

```
Dockerfile        the recipe for the image
.dockerignore     what to leave out of the box
requirements.txt  the Python packages the web app needs (and nothing else)
```

## The Dockerfile, line by line

```dockerfile
FROM python:3.10-slim
```
Start from an image someone else made: Debian Linux with Python 3.10 installed. `slim` leaves out compilers and extra tools, so
it is about 120 MB instead of 1 GB. 3.10 matches your local Python, so "works on my machine" means "works in the container".

```dockerfile
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1
```
`ENV` sets environment variables for the build and for the running container.
- `PYTHONUNBUFFERED=1`: `print()` output goes straight to `docker logs`. Without it, logs can arrive minutes late.
- `PYTHONDONTWRITEBYTECODE=1`: no `.pyc` files, which would only add size.
- `PIP_NO_CACHE_DIR=1`: pip keeps a download cache that is useless inside an image and would be stored in the layer.

```dockerfile
WORKDIR /app
```
Create `/app` and make it the current folder for every later instruction and for the running app. All paths below are relative to it.

```dockerfile
COPY requirements.txt .
RUN pip install -r requirements.txt
```
`COPY` puts a file from your computer into the image. `RUN` executes a command **while building** and saves the result as a layer.
This is the slow step (about 415 MB: pandas, pyarrow, duckdb, shapely...). Only `requirements.txt` is copied first, on purpose;
see "Why the order matters" below.

```dockerfile
COPY app ./app
COPY templates ./templates
COPY static ./static
COPY redfin ./redfin
```
Now the code. The folders keep the layout they have in the repo, which matters: `redfin/config.py` looks for
`redfin/data/neighbourhoods/` next to itself. `build_site.py` and `_site/` (the GitHub Pages build) are not copied.

```dockerfile
RUN pip install --no-deps -e ./redfin
```
Installs your own `redfin` package so `from redfin import config` works.
- `--no-deps`: the package's `pyproject.toml` lists playwright, beautifulsoup4 and requests, which the *scraper* needs. The web app
  does not, and everything it needs is already installed by the previous `pip install`.
- `-e` (editable): leaves the code in `/app/redfin/src` instead of copying it into Python's `site-packages`. This is required here: a normal
  install would move `config.py` away from `redfin/data/`, and the neighbourhood map would fail with "file not found".

```dockerfile
RUN useradd --create-home --uid 1000 appuser
USER appuser
```
By default everything in a container runs as `root`. Switching to an ordinary user means that if someone finds a bug in the app, they
cannot change system files. Everything after `USER` (including the running app) is `appuser`.

```dockerfile
ENV PORT=8000
EXPOSE 8000
```
`PORT` is the port the app listens on; Azure sets its own value at run time. `EXPOSE` is documentation only: it does **not** open the
port. `docker run -p` does that.

```dockerfile
CMD ["sh", "-c", "exec gunicorn app.main:app --bind 0.0.0.0:${PORT:-8000} --workers 1 --worker-class gthread --threads 4 --timeout 120 ..."]
```
What runs when a container starts (not at build time).
- `app.main:app` means "in the file `app/main.py`, the object called `app`".
- `--bind 0.0.0.0:${PORT:-8000}`: listen on all network interfaces, on `$PORT` or 8000 if it is unset. `127.0.0.1` would be unreachable
  from outside the container. We call `sh -c` so the shell expands `${PORT:-8000}`; the plain list form of `CMD` would not.
- `exec`: gunicorn replaces the shell as the container's main process, so `docker stop` signals gunicorn directly and it shuts down at once.
- `--workers 1 --threads 4`: **`app/main.py` downloads all the data from Azure when it is imported** (`load_data()` at module level),
  so every worker process would hold its own full copy (about 250 MB each). One process with four threads serves four requests at once
  from one copy. `/refresh-data` also changes variables inside one process, so with several workers it would update only one of them.
- `--timeout 120`: the Azure download happens while the worker starts, and gunicorn's default 30 s limit would kill a slow start.
- `--access-logfile - --error-logfile -`: print logs to the terminal, which is where `docker logs` reads them.

## Why the layer order matters (caching)

Docker runs the instructions top to bottom and saves each result as a layer. On the next build it reuses a layer if the instruction
**and everything before it** is unchanged. The first change invalidates every layer after it.

```
1. FROM, ENV, WORKDIR              never changes             -> cached
2. COPY requirements.txt + pip     changes when you add a    -> cached on a normal code edit (saves minutes)
                                   package
3. COPY app/templates/static/redfin changes on every edit     -> rebuilt, takes about a second
4. pip install --no-deps -e        reruns after 3            -> about 10 s, downloads nothing
```

If we had written `COPY . .` first and installed packages after, **every** edit to a `.js` file would reinstall pandas and pyarrow.
You can see it in the build output: unchanged layers say `CACHED`.

## .dockerignore: what it protects

When you run `docker build .`, Docker first sends the whole folder (the "build context") to the engine, and `COPY` can only take
from it. `.dockerignore` removes files from the context, so they cannot be copied by any `COPY` line, now or after a future edit.

Two reasons to leave something out:

**Secrets.** Every layer is stored in the image, and anyone who can pull the image can read every file in every layer. Deleting a file
in a later layer does *not* remove it from the earlier one.

| Entry | Why |
|---|---|
| `.env` | Holds `AZURE_STORAGE_CONNECTION_STRING`, a key to your storage account. It would be readable by anyone who gets the image. |
| `*cookies*.json`, `redfin/chrome_cookies.json` | Your logged-in Redfin session. Also `redfin_profile/`, the browser profile. |

**Size and speed** (a smaller context builds faster, and a changed file that is ignored cannot bust the cache):

| Entry | Why |
|---|---|
| `.git`, `.github` | The full history, often bigger than the code, and useless at run time. |
| `_site`, `build_site.py` | The GitHub Pages output (27 MB), not used by the web app. |
| `**/__pycache__`, `**/*.py[cod]`, `**/.pytest_cache`, `**/*.egg-info` | Machine-generated, and they change on every test run. The `**/` means "at any depth": a plain `__pycache__/` matches only the top level, so `redfin/.pytest_cache` slipped into our first build. |
| `.venv`, `venv`, `env` | Hundreds of MB, and built for Windows, so they do not run on Linux. |
| `redfin/tests`, `redfin/docs`, `redfin/scripts`, `redfin/data/queue`, `docs` | Not used by the web app. |
| `output`, `*.log` | Local scraper output (screenshots) and logs. |
| `.vscode`, `.idea`, `.claude`, `CLAUDE.md`, `.DS_Store`, `Thumbs.db` | Editor and assistant files. |
| `Dockerfile`, `.dockerignore` | Not needed inside the image. |

`.gitignore` and `.dockerignore` are separate: a file can be ignored by git yet still be sent to Docker (for example `.env`, if it
were not listed), which is the reason this file exists.

## How secrets get in at run time

The image has no secrets. A container gets them when it **starts**:

```powershell
docker run --rm -p 8000:8000 --env-file .env alexis-website
```

`--env-file .env` reads your local `.env` (`NAME=value` lines) and sets them as environment variables of that one container.
`redfin/config.py` reads `AZURE_STORAGE_CONNECTION_STRING` with `os.getenv`, which works the same whether the variable came from a
`.env` file, from this flag, or from Azure later. The `.env` file itself is never copied into the image.

On Azure the same variable is supplied from a secret (a stored, access-controlled value) instead of a file. The app code does not change.

Check that the image is clean: `docker run --rm --entrypoint sh alexis-website -c "find / -name '.env'"` should print nothing.
(Environment variables are also visible to `docker inspect` on the machine that runs the container, so treat that machine as trusted.)

## Everyday commands

Docker Desktop must be running. Run these from the repo root (PowerShell or Git Bash).

### Build

```powershell
docker build -t alexis-website .
```
Builds the image from the `Dockerfile` in `.` and names it (`-t`, "tag") `alexis-website`. Run it again after every code change.

### Run

```powershell
docker run --rm -p 8000:8000 --env-file .env alexis-website
```
- `-p 8000:8000`: **host port : container port**. The container's 8000 is reachable at http://localhost:8000. For port 9000 on your
  PC use `-p 9000:8000`.
- `--rm`: delete the container when it stops, so stopped containers do not pile up.
- Ctrl+C stops it. To run in the background, add `-d` and `--name alexis-web`:

```powershell
docker run -d --name alexis-web -p 8000:8000 --env-file .env alexis-website
```

Open http://localhost:8000/, http://localhost:8000/ottawa_map and http://localhost:8000/ottawa_active. The first request that needs
the neighbourhood table takes about 6 s (the app builds and caches it once).

### See what is happening

```powershell
docker ps                       # running containers (docker ps -a also shows stopped ones)
docker logs alexis-web          # everything printed so far
docker logs -f alexis-web       # follow live, Ctrl+C to stop following
docker logs --tail 50 alexis-web
```
Startup prints the parquet files found and `Registered 'properties' view with N rows`. Failures are printed as `ERROR in ...`.

### Open a shell inside the container

```powershell
docker exec -it alexis-web sh
```
You are now inside the running container, as `appuser`, in `/app`. Look around (`ls`, `pip list`), then `exit`. The container keeps
running. Changes you make in there are lost when it is removed: edit the code on your PC and rebuild instead. To poke at an image
that is not running: `docker run --rm -it --entrypoint sh alexis-website`.

### Memory and CPU

```powershell
docker stats                    # live table, Ctrl+C to leave
docker stats --no-stream        # one reading
docker exec alexis-web sh -c "cat /sys/fs/cgroup/memory.peak"   # highest memory since start, in bytes
```
Measured for this app with the data loaded and the maps used: **about 250 MB** steady, **about 290 MB** peak (the reload in `/refresh-data`
briefly holds two copies). Choose the Azure container size from the peak, with headroom (for example 1 GB).
It grows with the amount of data in `silver/`.

### Image size

```powershell
docker images alexis-website
```
`DISK USAGE` (about 780 MB) is the uncompressed size on your disk. `CONTENT SIZE` (about 197 MB) is what is uploaded to a registry.
`docker history alexis-website` shows the size of each layer; the `pip install -r requirements.txt` layer is by far the largest.

### Stop and clean up

```powershell
docker stop alexis-web          # graceful stop (a container started with --rm disappears)
docker rm alexis-web            # remove a stopped container (only needed if you did not use --rm)
docker rmi alexis-website       # delete the image

docker container prune          # delete ALL stopped containers
docker image prune              # delete dangling (untagged) images left by rebuilds
docker system df                # how much space Docker uses
docker builder prune            # empty the build cache (the next build is slow again)
```
Always stop containers **by name** (`docker stop alexis-web`), so you never touch containers that belong to something else.
`prune` commands ask for confirmation and affect everything on the machine, not just this project.

## Troubleshooting

| Symptom | Cause and fix |
|---|---|
| `failed to connect to the docker API ... dockerDesktopLinuxEngine` | Docker Desktop is not running. Start it, wait for the whale icon to stop animating. |
| `port is already allocated` / `address already in use` | Something else uses 8000, maybe an old container (`docker ps`) or the Flask dev server. Use `-p 8001:8000`. |
| Page loads but the map is empty, logs say "Running in offline/empty mode" | The connection string was not passed. Add `--env-file .env`, and check the file has `AZURE_STORAGE_CONNECTION_STRING=...` with no spaces around `=`. |
| Container exits at once | `docker logs <name>` shows why. Without `--rm`, the stopped container is kept so you can read its logs. |
| Build is slow every time | A line above `pip install` changed, or `COPY . .` was added before it. Keep `requirements.txt` copied alone first. |
| `ModuleNotFoundError` in the container | The package is missing from `requirements.txt`. It works on your PC because your PC has more packages installed. Add it and rebuild. |
| You changed a file and the container shows the old version | A container runs a snapshot of the image. Rebuild, stop the old container, run again. |
