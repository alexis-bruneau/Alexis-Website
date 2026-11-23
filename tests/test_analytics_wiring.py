"""The analytics hook as wired into app/main.py: which real routes count, and where the visitor address comes from."""
import pytest

from app import analytics

CHROME = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"


@pytest.fixture(scope="module")
def main_app():
    """app.main with no Azure: an empty connection string means the data loaders run offline (and load_dotenv does not
    override a variable that is already set, so a developer's .env cannot make this test download the real data)."""
    mp = pytest.MonkeyPatch()
    mp.setenv("AZURE_STORAGE_CONNECTION_STRING", "")
    try:
        from app import main
    finally:
        mp.undo()
    return main


@pytest.fixture
def client(main_app, monkeypatch):
    jobs = []
    monkeypatch.setenv("AZURE_STORAGE_CONNECTION_STRING", "UseDevelopmentStorage=true;FAKE")
    monkeypatch.setattr(analytics, "_enqueue", lambda job: jobs.append(job) or True)
    c = main_app.app.test_client()
    c.jobs = jobs
    return c


def get(client, path, host="quartierinsights.ca", **headers):
    return client.get(path, base_url=f"http://{host}", headers={"User-Agent": CHROME, **headers})


def test_the_sold_map_is_counted(client):
    assert get(client, "/ottawa_map").status_code == 200
    assert [j["path"] for j in client.jobs] == ["/ottawa_map"]


def test_the_front_door_redirect_counts_once_at_the_map(client):
    response = get(client, "/", Referer="https://www.reddit.com/r/ottawa/")
    assert response.status_code == 302
    assert client.jobs == []                                  # the 302 is not a view; the browser now asks for /ottawa_map
    get(client, "/ottawa_map", Referer="https://www.reddit.com/r/ottawa/")
    assert [j["referrer_host"] for j in client.jobs] == ["www.reddit.com"]


def test_the_portfolio_home_page_is_not_counted(client):
    assert get(client, "/", host="ca-alexis-web.azurecontainerapps.io").status_code == 200
    assert client.jobs == []


@pytest.mark.parametrize("path", ["/ottawa_active", "/points.json", "/neighbourhood-boundaries.geojson", "/static/css/map.css"])
def test_everything_else_is_not_counted(client, path):
    get(client, path)
    assert client.jobs == []


def test_the_visitor_address_is_the_last_forwarded_entry(client):
    """The ingress appends the real address; anything the visitor typed into the header comes before it."""
    get(client, "/ottawa_map", **{"X-Forwarded-For": "6.6.6.6, 203.0.113.9"})
    assert client.jobs[0]["ip"] == "203.0.113.9"


def test_without_a_proxy_header_the_connection_address_is_used(client):
    get(client, "/ottawa_map")
    assert client.jobs[0]["ip"] == "127.0.0.1"


def test_the_host_and_scheme_are_not_taken_from_the_proxy(client):
    response = get(client, "/", **{"X-Forwarded-Host": "evil.example", "X-Forwarded-Proto": "https"})
    assert response.headers["Location"] == "/ottawa_map"      # still the relative redirect of the map host
