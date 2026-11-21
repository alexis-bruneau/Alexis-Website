"""Tests for redfin.daily: step order, failure rules, flags, exit code and summary. subprocess.call is mocked, so nothing
runs and nothing touches redfin.ca or Azure. Run:  python -m pytest redfin"""
import contextlib
import io
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from redfin import config, daily, runstats

ALL_SIX = ["redfin.extract.discover_sold", "redfin.extract.fetch_sold", "redfin.transform.sold",
           "redfin.extract.census", "redfin.extract.fetch_active", "redfin.transform.active"]


class FakeRedfin:
    """Stands in for subprocess.call: records each step, returns scripted exit codes, and writes the counts a real step would."""

    def __init__(self, urls_file, codes=None, new_urls=0, stats=None):
        self.urls_file, self.codes, self.new_urls = urls_file, codes or {}, new_urls
        self.stats = stats or {}
        self.calls = []   # (module, args, env)

    def __call__(self, cmd, env=None):
        assert cmd[1] == "-m"
        module, args = cmd[2], cmd[3:]
        self.calls.append((module, args, env))
        if module == daily.DISCOVER and self.new_urls:
            with self.urls_file.open("a", encoding="utf-8") as f:
                f.writelines(f"https://www.redfin.ca/on/ottawa/new-{i}/home/{i}\n" for i in range(self.new_urls))
        step = {daily.FETCH_SOLD: "fetch_sold", daily.TRANSFORM_SOLD: "transform.sold", daily.CENSUS: "census",
                daily.FETCH_ACTIVE: "fetch_active", daily.TRANSFORM_ACTIVE: "transform.active"}.get(module)
        if step in self.stats:
            path = Path(env["REDFIN_STATS_FILE"])
            data = json.loads(path.read_text() or "{}")
            data[step] = self.stats[step]
            path.write_text(json.dumps(data))
        return self.codes.get(module, 0)

    @property
    def modules(self):
        return [m for m, _, _ in self.calls]

    def call(self, module):
        return next(c for c in self.calls if c[0] == module)


STATS = {"fetch_sold": {"saved": 187, "stopped_early": False}, "transform.sold": {"rows_added": 175},
         "census": {"listings": 4951, "header_total": 4951, "complete": True},
         "fetch_active": {"saved": 200, "stopped_early": False},
         "transform.active": {"rows_written": 1204, "removed_sold": 3}}


class DailyTest(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.urls_file = Path(tmp.name) / "property_urls.txt"
        self.urls_file.write_text("https://www.redfin.ca/on/ottawa/old/home/1\n", encoding="utf-8")
        patch = mock.patch.object(config, "URLS_FILE", self.urls_file)
        patch.start()
        self.addCleanup(patch.stop)

    def run_daily(self, *argv, codes=None, new_urls=0, stats=None):
        fake = FakeRedfin(self.urls_file, codes, new_urls, STATS if stats is None else stats)
        out = io.StringIO()
        with mock.patch("subprocess.call", fake), contextlib.redirect_stdout(out):
            code = daily.main(list(argv))
        return code, fake, out.getvalue()

    # ---- order and arguments -------------------------------------------------------------

    def test_steps_run_in_the_documented_order(self):
        code, fake, _ = self.run_daily()
        self.assertEqual(fake.modules, ALL_SIX)
        self.assertEqual(code, 0)

    def test_step_arguments_and_limits(self):
        _, fake, _ = self.run_daily()
        self.assertEqual(fake.call(daily.FETCH_SOLD)[2]["MAX_SCRAPE_COUNT"], "200")
        self.assertEqual(fake.call(daily.TRANSFORM_SOLD)[1], ["--incremental", "--out-prefix", "silver"])
        self.assertEqual(fake.call(daily.FETCH_ACTIVE)[1], ["--n", "200"])
        self.assertEqual(fake.call(daily.TRANSFORM_ACTIVE)[1], [])
        self.assertEqual(fake.call(daily.CENSUS)[1], [])

    def test_limit_flags(self):
        _, fake, _ = self.run_daily("--limit-sold", "30", "--limit-active", "40")
        self.assertEqual(fake.call(daily.FETCH_SOLD)[2]["MAX_SCRAPE_COUNT"], "30")
        self.assertEqual(fake.call(daily.FETCH_ACTIVE)[1], ["--n", "40"])

    def test_every_step_gets_utf8_and_the_stats_file_and_the_real_environment(self):
        with mock.patch.dict(os.environ, {"AZURE_STORAGE_CONNECTION_STRING": "x"}):
            _, fake, _ = self.run_daily()
        for _, _, env in fake.calls:
            self.assertEqual(env["PYTHONUTF8"], "1")
            self.assertIn("REDFIN_STATS_FILE", env)
            self.assertEqual(env["AZURE_STORAGE_CONNECTION_STRING"], "x")

    def test_the_stats_file_is_removed_afterwards(self):
        _, fake, _ = self.run_daily()
        self.assertFalse(Path(fake.calls[0][2]["REDFIN_STATS_FILE"]).exists())

    # ---- skip flags ----------------------------------------------------------------------

    def test_skip_sold_runs_only_the_active_block(self):
        code, fake, _ = self.run_daily("--skip-sold")
        self.assertEqual(fake.modules, ALL_SIX[3:])
        self.assertEqual(code, 0)

    def test_skip_active_runs_only_the_sold_block(self):
        code, fake, out = self.run_daily("--skip-active")
        self.assertEqual(fake.modules, ALL_SIX[:3])
        self.assertEqual(code, 0)
        self.assertNotIn("census complete", out)

    def test_skipping_both_blocks_is_an_error(self):
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as cm:
            self.run_daily("--skip-sold", "--skip-active")
        self.assertEqual(cm.exception.code, 2)

    # ---- failure rules -------------------------------------------------------------------

    def test_discovery_failure_skips_fetch_sold_but_not_the_rest(self):
        code, fake, _ = self.run_daily(codes={daily.DISCOVER: 2})
        self.assertEqual(fake.modules, [m for m in ALL_SIX if m != daily.FETCH_SOLD])
        self.assertEqual(code, 1)

    def test_fetch_sold_failure_still_transforms_and_runs_the_active_block(self):
        code, fake, _ = self.run_daily(codes={daily.FETCH_SOLD: 1})
        self.assertEqual(fake.modules, ALL_SIX)
        self.assertEqual(code, 1)

    def test_sold_transform_failure_does_not_stop_the_active_block(self):
        code, fake, _ = self.run_daily(codes={daily.TRANSFORM_SOLD: 1})
        self.assertEqual(fake.modules, ALL_SIX)
        self.assertEqual(code, 1)

    def test_a_failing_active_block_does_not_undo_the_sold_block(self):
        code, fake, _ = self.run_daily(codes={daily.CENSUS: 1, daily.FETCH_ACTIVE: 1})
        self.assertEqual(fake.modules[:3], ALL_SIX[:3])
        self.assertEqual(code, 1)

    def test_incomplete_census_still_fetches_and_transforms_and_says_so(self):
        stats = {**STATS, "census": {"listings": 3812, "header_total": 4951, "complete": False}}
        code, fake, out = self.run_daily(codes={daily.CENSUS: 2}, stats=stats)
        self.assertEqual(fake.modules, ALL_SIX)
        self.assertEqual(code, 1)
        self.assertIn("INCOMPLETE", out)
        self.assertIn("census complete          NO, incomplete (3,812 listings saved)", out)

    def test_failed_census_skips_fetch_active_and_still_transforms(self):
        code, fake, out = self.run_daily(codes={daily.CENSUS: 1})
        self.assertEqual(fake.modules, [m for m in ALL_SIX if m != daily.FETCH_ACTIVE])
        self.assertEqual(code, 1)
        self.assertIn("the census step failed", out)

    def test_fetch_active_failure_still_runs_the_active_transform(self):
        code, fake, _ = self.run_daily(codes={daily.FETCH_ACTIVE: 2})
        self.assertEqual(fake.modules, ALL_SIX)
        self.assertEqual(code, 1)

    def test_exit_code_is_zero_only_when_every_step_succeeded(self):
        for module in ALL_SIX:
            with self.subTest(failing=module):
                self.assertEqual(self.run_daily(codes={module: 1})[0], 1)

    # ---- summary -------------------------------------------------------------------------

    def test_summary_of_a_normal_run(self):
        _, _, out = self.run_daily(new_urls=37)
        summary = out[out.index("=== summary ==="):]
        for expected in ("new URLs queued          37", "sold pages fetched       187", "sold rows added          175",
                         "census complete          yes (4,951 listings; Redfin's header said 4,951)",
                         "active pages fetched     200", "active rows written      1,204", "removed because sold     3"):
            self.assertIn(expected, summary)

    def test_every_step_prints_a_header(self):
        _, _, out = self.run_daily()
        for module in ALL_SIX:
            self.assertIn(f"=== {module} ===", out)

    def test_summary_flags_a_fetch_that_stopped_early(self):
        stats = {**STATS, "fetch_sold": {"saved": 12, "stopped_early": True}}
        _, _, out = self.run_daily(stats=stats)
        self.assertIn("sold pages fetched       12   (stopped early", out)

    def test_missing_counts_show_as_na_not_as_zero(self):
        _, _, out = self.run_daily(stats={})
        self.assertIn("sold pages fetched       n/a", out)
        self.assertIn("removed because sold     n/a", out)


class RunStatsTest(unittest.TestCase):
    def test_report_merges_steps_into_one_file_and_is_a_no_op_when_unset(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "stats.json"
            with mock.patch.object(config, "STATS_FILE", None):
                runstats.report("fetch_sold", saved=1)
            self.assertFalse(path.exists())
            with mock.patch.object(config, "STATS_FILE", str(path)):
                runstats.report("fetch_sold", saved=1)
                runstats.report("fetch_sold", stopped_early=False)
                runstats.report("census", listings=5)
            self.assertEqual(runstats.read(path), {"fetch_sold": {"saved": 1, "stopped_early": False}, "census": {"listings": 5}})

    def test_read_of_a_missing_or_broken_file_is_empty(self):
        with tempfile.TemporaryDirectory() as tmp:
            self.assertEqual(runstats.read(Path(tmp) / "nope.json"), {})
            bad = Path(tmp) / "bad.json"
            bad.write_text("{not json", encoding="utf-8")
            self.assertEqual(runstats.read(bad), {})


if __name__ == "__main__":
    unittest.main()
