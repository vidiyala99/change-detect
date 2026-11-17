"""serial queue (5 waiting), meters, COSTS, seeded background load, mutator, hook between round trips."""
import json
import unittest

from changedet.source import (COSTS, FIELDS, BackgroundLoad, Mutator, QueueFull, Source, apply_ops,
                              make_rows)


def one_row_handler(source):
    def handler(conn):
        rows = source.all_rows()
        return rows[:1], len(rows)
    return handler


class TableTest(unittest.TestCase):
    def test_crud_and_null_values(self):
        s = Source(make_rows(3, seed=1))
        self.assertEqual(s.count(), 3)
        s.update(2, memo=None, amount=7)
        self.assertEqual(s.get(2)["memo"], None)
        self.assertEqual(s.get(2)["amount"], 7)
        s.delete(1)
        new_id = s.insert({f: "x" if f != "amount" else 1 for f in FIELDS})
        self.assertEqual(sorted(r["id"] for r in s.all_rows()), sorted([2, 3, new_id]))
        self.assertIsNone(s.get(1))

    def test_make_rows_is_seeded(self):
        self.assertEqual(make_rows(50, seed=4), make_rows(50, seed=4))
        self.assertNotEqual(make_rows(50, seed=4), make_rows(50, seed=5))
        self.assertEqual(set(make_rows(1)[0]), {"id", *FIELDS})


class MeterTest(unittest.TestCase):
    def test_request_meters_bytes_scan_and_queue_time(self):
        s = Source(make_rows(10, seed=1))
        payload = s.request(one_row_handler(s))
        wire = len(json.dumps(payload, separators=(",", ":")).encode())
        m = s.meters
        self.assertEqual(m["requests"], 1)
        self.assertEqual(m["bytes"], wire)
        self.assertEqual(m["rows_scanned_local"], 10)
        expected = (COSTS["request_latency_ms"] + 10 * COSTS["local_scan_ms_per_row"]
                    + wire / COSTS["wire_bytes_per_ms"])
        self.assertAlmostEqual(m["queue_held_ms"], expected)
        self.assertAlmostEqual(s.clock.now, expected)

    def test_scan_does_not_hold_queue_when_costs_say_so(self):
        costs = dict(COSTS, local_scan_holds_queue=False)
        s = Source(make_rows(10, seed=1), costs=costs)
        s.request(one_row_handler(s))
        wire = s.meters["bytes"]
        self.assertAlmostEqual(s.meters["queue_held_ms"],
                               COSTS["request_latency_ms"] + wire / COSTS["wire_bytes_per_ms"])

    def test_costs_block_declares_every_assumption(self):
        for k in ("wire_encoding", "hash_bytes_on_wire", "request_latency_ms", "local_scan_ms_per_row",
                  "wire_bytes_per_ms", "local_scan_holds_queue", "max_waiting", "backoff_ms"):
            self.assertIn(k, COSTS)
        self.assertEqual(COSTS["max_waiting"], 5)
        json.dumps(COSTS)  # printable verbatim


class QueueTest(unittest.TestCase):
    def test_sixth_waiting_request_is_rejected(self):
        # 1 running + 5 waiting background jobs: our request would be the 6th waiting.
        bg = BackgroundLoad(arrivals=[(0.0, 1000.0)] * 6)
        s = Source(make_rows(2), background=bg)
        s.clock.now = 1.0
        with self.assertRaises(QueueFull):
            s.request(one_row_handler(s))
        self.assertEqual(s.meters["queue_full"], 1)
        self.assertEqual(s.meters["requests"], 0)

    def test_fifth_waiting_request_is_accepted_and_waits_its_turn(self):
        bg = BackgroundLoad(arrivals=[(0.0, 1000.0)] * 5)  # 1 running, 4 waiting
        s = Source(make_rows(2), background=bg)
        s.clock.now = 1.0
        s.request(one_row_handler(s))
        self.assertEqual(s.meters["requests"], 1)
        # we start after all five background jobs finish
        self.assertAlmostEqual(s.clock.now, 5000.0 + s.meters["queue_held_ms"])

    def test_background_beyond_limit_is_dropped_not_queued(self):
        bg = BackgroundLoad(arrivals=[(0.0, 1000.0)] * 9)  # 3 dropped
        s = Source(make_rows(2), background=bg)
        s.clock.now = 4500.0  # jobs 1-4 done, 5 running, job 6 waiting -> accepted
        s.request(one_row_handler(s))
        self.assertAlmostEqual(s.clock.now, 6000.0 + s.meters["queue_held_ms"])

    def test_queue_drains_over_time(self):
        bg = BackgroundLoad(arrivals=[(0.0, 1000.0)] * 6)
        s = Source(make_rows(2), background=bg)
        s.clock.now = 1001.0
        s.request(one_row_handler(s))
        self.assertEqual(s.meters["requests"], 1)

    def test_seeded_background_is_reproducible_and_fills_the_queue(self):
        def run(seed):
            s = Source(make_rows(5), background=BackgroundLoad.seeded(seed, mean_gap_ms=200, service_ms=250))
            outcomes = []
            for _ in range(60):
                try:
                    s.request(one_row_handler(s))
                    outcomes.append("ok")
                    s.clock.now += 1000.0  # idle between polls: the queue refills
                except QueueFull:
                    outcomes.append("full")
                    s.clock.now += COSTS["backoff_ms"]
            return outcomes, s.clock.now
        a, b = run(3), run(3)
        self.assertEqual(a, b)
        self.assertIn("full", a[0])
        self.assertIn("ok", a[0])


class HookTest(unittest.TestCase):
    def test_after_request_hooks_run_between_round_trips(self):
        s = Source(make_rows(3))
        seen = []
        s.after_request.append(lambda src: seen.append(src.meters["requests"]))
        s.request(one_row_handler(s))
        s.request(one_row_handler(s))
        self.assertEqual(seen, [1, 2])

    def test_direct_edits_are_not_metered(self):
        s = Source(make_rows(3))
        s.update(1, amount=1)
        s.delete(2)
        self.assertEqual(s.meters["requests"], 0)
        self.assertEqual(s.meters["bytes"], 0)


class MutatorTest(unittest.TestCase):
    def test_seeded_ops_are_reproducible_and_replayable(self):
        a, b = Source(make_rows(200, seed=1)), Source(make_rows(200, seed=1))
        ops_a = Mutator(seed=9).step(a, rate=0.1)
        ops_b = Mutator(seed=9).step(b, rate=0.1)
        self.assertEqual(ops_a, ops_b)
        self.assertEqual(len(ops_a), 20)
        kinds = {op[0] for op in ops_a}
        self.assertEqual(kinds, {"create", "update", "delete"})
        c = Source(make_rows(200, seed=1))
        apply_ops(c, ops_a)
        self.assertEqual(c.all_rows(), a.all_rows())

    def test_rate_zero_changes_nothing_and_small_rates_round_up(self):
        s = Source(make_rows(100))
        self.assertEqual(Mutator(seed=1).step(s, rate=0.0), [])
        self.assertEqual(len(Mutator(seed=1).step(s, rate=0.001)), 1)

    def test_empty_table_starts_with_a_create(self):
        s = Source([])
        ops = Mutator(seed=1).step(s, rate=0.5, min_ops=3)
        self.assertEqual(len(ops), 3)
        self.assertEqual(ops[0][0], "create")
        replay = Source([])
        apply_ops(replay, ops)
        self.assertEqual(replay.all_rows(), s.all_rows())


if __name__ == "__main__":
    unittest.main()
