"""Build the static portfolio site into ./_site for GitHub Pages.

Run locally with `python build_site.py`, then preview with
`python -m http.server --directory _site`.
"""
import re
import shutil
from pathlib import Path

ROOT = Path(__file__).parent
OUT = ROOT / "_site"

# Pages that need the Flask/Azure backend (or aren't ready) stay out of the public site.
EXCLUDE_TEMPLATES = {"ottawa_map.html", "ottawa_active.html", "ASL.html", "Webscraping.html"}
EXCLUDE_STATIC = {"js/map.js", "js/neighbourhoods.js", "js/active.js", "css/map.css", "js/asl.js", "js/webscraper.js"}

# The real-estate dashboard runs on Azure (Container App ca-alexis-web) under its own domain. The published menu links
# there; change this one line if the address changes.
DASHBOARD_URL = "https://quartierinsights.ca/ottawa_map"


def main():
    if OUT.exists():
        shutil.rmtree(OUT)
    OUT.mkdir()

    # templates/ -> site root (index.html, menu_bar.html, Project_PDF_Reports/, ...)
    for src in (ROOT / "templates").iterdir():
        if src.name in EXCLUDE_TEMPLATES:
            continue
        if src.is_dir():
            shutil.copytree(src, OUT / src.name)
        else:
            shutil.copy2(src, OUT / src.name)

    # The static site has no backend, so the published menu sends the dashboard link to the Azure app
    # (the source menu keeps the relative link for the local Flask app and the Azure app itself).
    menu = OUT / "menu_bar.html"
    html = menu.read_text(encoding="utf-8")
    html, n = re.subn(r'<a href="ottawa_map">', f'<a href="{DASHBOARD_URL}" target="_blank" rel="noopener">', html, count=1)
    assert n == 1, "dashboard link not found in menu_bar.html"
    menu.write_text(html, encoding="utf-8")

    # static/ -> site/static
    static_out = OUT / "static"
    shutil.copytree(
        ROOT / "static",
        static_out,
        ignore=lambda d, names: [
            n for n in names
            if (Path(d) / n).relative_to(ROOT / "static").as_posix() in EXCLUDE_STATIC
        ],
    )

    # Stop GitHub from running Jekyll over the files (breaks names with spaces/underscores).
    (OUT / ".nojekyll").touch()
    print(f"Built site in {OUT}")


if __name__ == "__main__":
    main()
