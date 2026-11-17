"""identical trace per strategy, bootstrap apart, crossover as a bracket, COSTS in bench.json."""
import json
import os
import tempfile
import unittest

from changedet.bench import crossover_bracket, run, write
from changedet.source import COSTS

SMALL = dict(n_rows=300, rates=[0.01, 0.5], bucket_counts=[8, 64], cycles=2, seed=1)


class CrossoverTest(unittest.TestCase):
    RATES = [0.001, 0.01, 0.1, 0.5]

    def test_bracket_between_tested_rates(self):
        c = crossover_bracket(self.RATES, [1, 2, 5, 10], [5, 5, 5, 5])
        self.assertEqual(c["bracket"], [0.01, 0.1])
        self.assertEqual(c["a_wins_at"], [0.001, 0.01])
        self.assertIn("between 1% and 10%", c["summary"])

    def test_a_wins_everywhere(self):
        c = crossover_bracket(self.RATES, [1, 1, 1, 1], [2, 2, 2, 2])
        self.assertIsNone(c["bracket"])
        self.assertIn("every tested rate", c["summary"])

    def test_a_never_wins(self):
        c = crossover_bracket(self.RATES, [3, 3, 3, 3], [2, 2, 2, 2])
        self.assertIsNone(c["bracket"])
        self.assertEqual(c["a_wins_at"], [])
        self.assertIn("no tested rate", c["summary"])

    def test_tie_is_not_a_win(self):
        c = crossover_bracket(self.RATES, [1, 5, 5, 5], [5, 5, 5, 5])
        self.assertEqual(c["bracket"], [0.001, 0.01])

    def test_non_monotonic_is_reported(self):
        c = crossover_bracket(self.RATES, [1, 9, 1, 9], [5, 5, 5, 5])
        self.assertFalse(c["monotonic"])
        self.assertEqual(c["a_wins_at"], [0.001, 0.1])


class RunTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.result = run(**SMALL, background_rate=0.01)

    def test_costs_printed_verbatim(self):
        self.assertEqual(self.result["costs"], COSTS)

    def test_every_strategy_sees_the_identical_trace(self):
        for rate, digests in self.result["trace_digests"].items():
            with self.subTest(rate=rate):
                self.assertGreaterEqual(len(digests), 4)
                self.assertEqual(len(set(map(json.dumps, digests.values()))), 1, digests)

    def test_bootstrap_is_reported_apart_from_steady_state(self):
        names = {r["strategy"] for r in self.result["results"]}
        self.assertEqual(names, {"FullRefresh", "KeyHashSnapshot", "BucketDiff/8", "BucketDiff/64", "LocalSnapshot"})
        for r in self.result["results"]:
            self.assertEqual(r["bootstrap"]["rows_fetched"], 300)
            self.assertEqual(r["steady"]["cycles"], 2)
            for k in ("requests", "bytes", "queue_held_ms", "rows_scanned_local", "elapsed_ms"):
                self.assertIn(k, r["steady"])
                self.assertIn(k, r["bootstrap"])
            self.assertTrue(r["verified"], "cache must equal the source after every quiet cycle")

    def test_steady_state_bytes_rank_as_expected_at_low_change_rate(self):
        by = {r["strategy"]: r["steady"]["bytes"] for r in self.result["results"] if r["rate"] == 0.01}
        self.assertLess(by["LocalSnapshot"], by["KeyHashSnapshot"])
        self.assertLess(by["KeyHashSnapshot"], by["FullRefresh"])
        self.assertLess(by["BucketDiff/64"], by["FullRefresh"])

    def test_crossover_for_each_bucket_count_and_metric(self):
        for b in ("BucketDiff/8", "BucketDiff/64"):
            for metric in ("bytes", "queue_held_ms"):
                c = self.result["crossover"][b][metric]
                self.assertIn("summary", c)
                self.assertIn("bracket", c)

    def test_background_run_exercises_the_retry_path(self):
        bg = self.result["background_run"]
        self.assertEqual(bg["rate"], 0.01)
        self.assertGreater(sum(r["queue_full"] for r in bg["results"]), 0)
        self.assertTrue(all(r["verified"] for r in bg["results"]))

    def test_write_produces_valid_json(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "bench.json")
            write(self.result, path)
            with open(path, encoding="utf-8") as f:
                self.assertEqual(json.load(f)["costs"], COSTS)


if __name__ == "__main__":
    unittest.main()
