"""four strategies against the FullRefresh oracle, value-bearing events, mid-cycle changes,
QueueFull retry under background load, coalescing between polls."""
import random
import unittest

from changedet.detect import BucketDiff, FullRefresh, KeyHashSnapshot, LocalSnapshot, diff_event, replay
from changedet.source import FIELDS, MODEL, BackgroundLoad, Mutator, Source, make_rows

FACTORIES = {
    "FullRefresh": lambda s: FullRefresh(s),
    "KeyHashSnapshot": lambda s: KeyHashSnapshot(s),
    "BucketDiff/8": lambda s: BucketDiff(s, n_buckets=8),
    "BucketDiff/64": lambda s: BucketDiff(s, n_buckets=64),
    "LocalSnapshot": lambda s: LocalSnapshot(s),
}


def truth(source):
    return {r["id"]: r for r in source.all_rows()}


class OracleMixin:
    def assert_cycle(self, strat, source, touched=None):
        """Run one quiet cycle and check the oracle: cache == source, events replay old -> new, no false events."""
        before = {k: dict(v) for k, v in strat.cache.rows.items()}
        events = strat.cycle()
        self.assertEqual(strat.cache.rows, truth(source))
        self.assertEqual(replay(before, events), strat.cache.rows)
        ids = [e["id"] for e in events]
        self.assertEqual(len(ids), len(set(ids)), "one event per row per cycle (changes coalesce)")
        if touched is not None:
            self.assertLessEqual(set(ids), set(touched), "an untouched row produced an event")
        for e in events:
            if e["event"] == "updated":
                self.assertTrue(e["changed_fields"])
                for f, v in e["changed_fields"].items():
                    self.assertNotEqual(before[e["id"]][f], v)
        return events


class OracleTest(OracleMixin, unittest.TestCase):
    def test_random_rounds_match_oracle(self):
        for name, make in FACTORIES.items():
            with self.subTest(strategy=name):
                rng = random.Random(42)
                source = Source(make_rows(60, seed=7))
                strat = make(source)
                self.assert_cycle(strat, source)  # bootstrap
                mut = Mutator(seed=11)
                for _ in range(200):
                    rate = rng.choice([0.0, 0.01, 0.05, 0.2, 1.0])
                    ops = mut.step(source, rate)
                    touched = {op[1]["id"] if op[0] == "create" else op[1] for op in ops}
                    self.assert_cycle(strat, source, touched)

    def test_bootstrap_emits_created_for_every_row(self):
        for name, make in FACTORIES.items():
            with self.subTest(strategy=name):
                source = Source(make_rows(30, seed=2))
                events = make(source).cycle()
                self.assertEqual(sorted(e["id"] for e in events), list(range(1, 31)))
                self.assertEqual({e["event"] for e in events}, {"created"})

    def test_named_cases(self):
        def insert_only(s):
            for _ in range(5):
                s.insert({f: (1 if f == "amount" else "n") for f in FIELDS})

        def delete_only(s):
            for k in s.ids()[::3]:
                s.delete(k)

        def update_only(s):
            for k in s.ids()[::4]:
                s.update(k, amount=-1, memo=None)

        def all_rows_changed(s):
            for k in s.ids():
                s.update(k, status="changed-" + str(k))

        def delete_everything(s):
            for k in s.ids():
                s.delete(k)

        cases = [insert_only, delete_only, update_only, all_rows_changed, delete_everything]
        for name, make in FACTORIES.items():
            for case in cases:
                with self.subTest(strategy=name, case=case.__name__):
                    source = Source(make_rows(40, seed=3))
                    strat = make(source)
                    strat.cycle()
                    case(source)
                    self.assert_cycle(strat, source)
                    self.assertEqual(strat.cycle(), [], "a second quiet cycle is silent")

    def test_empty_table(self):
        for name, make in FACTORIES.items():
            with self.subTest(strategy=name):
                source = Source([])
                strat = make(source)
                self.assertEqual(strat.cycle(), [])
                k = source.insert({f: (5 if f == "amount" else "a") for f in FIELDS})
                self.assertEqual([e["event"] for e in self.assert_cycle(strat, source)], ["created"])
                source.delete(k)
                self.assertEqual([e["event"] for e in self.assert_cycle(strat, source)], ["deleted"])
                self.assertEqual(strat.cache.rows, {})


class CoalesceTest(OracleMixin, unittest.TestCase):
    def test_flip_back_between_polls_gives_no_event(self):
        for name, make in FACTORIES.items():
            with self.subTest(strategy=name):
                source = Source(make_rows(20, seed=1))
                strat = make(source)
                strat.cycle()
                old = source.get(5)
                source.update(5, amount=old["amount"] + 1)
                source.update(5, amount=old["amount"])
                self.assertEqual(strat.cycle(), [])

    def test_create_then_delete_between_polls_gives_no_event(self):
        for name, make in FACTORIES.items():
            with self.subTest(strategy=name):
                source = Source(make_rows(20, seed=1))
                strat = make(source)
                strat.cycle()
                k = source.insert({f: (1 if f == "amount" else "tmp") for f in FIELDS})
                source.delete(k)
                self.assertEqual(strat.cycle(), [])

    def test_several_updates_coalesce_into_one_event_with_final_values(self):
        for name, make in FACTORIES.items():
            with self.subTest(strategy=name):
                source = Source(make_rows(20, seed=1))
                strat = make(source)
                strat.cycle()
                source.update(3, memo="first")
                source.update(3, memo="second", status="paid-x")
                events = self.assert_cycle(strat, source)
                self.assertEqual(len(events), 1)
                self.assertEqual(events[0]["changed_fields"], {"memo": "second", "status": "paid-x"})


class MidCycleTest(OracleMixin, unittest.TestCase):
    """a row changed between round trips converges after one quiet cycle, even if it later flips back."""

    def test_change_between_round_trips_then_flip_back_converges(self):
        for name, make in FACTORIES.items():
            for after_request in (1, 2, 3):
                with self.subTest(strategy=name, after_request=after_request):
                    source = Source(make_rows(40, seed=5))
                    strat = make(source)
                    strat.cycle()
                    key = 9
                    v1 = {"amount": 111, "memo": "v1"}
                    v2 = {"amount": 222, "memo": "v2"}
                    source.update(key, **v1)  # the cycle will notice this row
                    start = source.meters["requests"]
                    fired = []

                    def hook(src, start=start, after_request=after_request):
                        if not fired and src.meters["requests"] - start == after_request:
                            fired.append(True)
                            src.update(key, **v2)
                    source.after_request.append(hook)
                    strat.cycle()  # not quiet: the oracle is not asserted here
                    source.after_request.remove(hook)
                    source.update(key, **v1)  # flip back before the next poll
                    self.assert_cycle(strat, source)
                    self.assertEqual(strat.cycle(), [])

    def test_random_mid_cycle_mutations_converge(self):
        for name, make in FACTORIES.items():
            with self.subTest(strategy=name):
                rng = random.Random(8)
                source = Source(make_rows(50, seed=6))
                strat = make(source)
                strat.cycle()
                mut = Mutator(seed=21)
                hook = lambda src: mut.step(src, 0.05) if rng.random() < 0.5 else None
                for _ in range(40):
                    mut.step(source, rng.choice([0.0, 0.05, 0.3]))
                    source.after_request.append(hook)
                    strat.cycle()
                    source.after_request.remove(hook)
                    self.assert_cycle(strat, source)


class QueueFullRetryTest(OracleMixin, unittest.TestCase):
    def test_strategies_retry_under_background_load(self):
        for name, make in FACTORIES.items():
            with self.subTest(strategy=name):
                bg = BackgroundLoad.seeded(4, mean_gap_ms=50, service_ms=250)
                source = Source(make_rows(50, seed=4), background=bg)
                strat = make(source)
                mut = Mutator(seed=2)
                self.assert_cycle(strat, source)
                for _ in range(15):
                    source.clock.now += 5000.0  # poll interval: other clients fill the queue meanwhile
                    mut.step(source, 0.1)
                    self.assert_cycle(strat, source)
                self.assertGreater(source.meters["queue_full"], 0, "the retry path never ran")


class EventShapeTest(unittest.TestCase):
    def test_payloads(self):
        row = {"id": 4, "vendor": "V", "job": "J", "cost_code": "C", "amount": 10, "status": "open", "memo": "m"}
        created = diff_event(None, row)
        self.assertEqual(created, {"event": "created", "model": MODEL, "id": 4, "data": row})
        changed = dict(row, amount=11, memo=None)
        self.assertEqual(diff_event(row, changed),
                         {"event": "updated", "model": MODEL, "id": 4, "changed_fields": {"amount": 11, "memo": None}})
        self.assertEqual(diff_event(row, None), {"event": "deleted", "model": MODEL, "id": 4})
        self.assertIsNone(diff_event(row, dict(row)))
        self.assertIsNone(diff_event(None, None))

    def test_created_payload_is_a_copy(self):
        row = {"id": 1, **{f: "x" for f in FIELDS}}
        e = diff_event(None, row)
        row["vendor"] = "mutated"
        self.assertEqual(e["data"]["vendor"], "x")

    def test_replay(self):
        old = {1: {"id": 1, **{f: "a" for f in FIELDS}}, 2: {"id": 2, **{f: "b" for f in FIELDS}}}
        new_row = {"id": 3, **{f: "c" for f in FIELDS}}
        events = [diff_event(old[1], dict(old[1], memo=None)), diff_event(old[2], None), diff_event(None, new_row)]
        out = replay(old, events)
        self.assertEqual(out, {1: dict(old[1], memo=None), 3: new_row})
        self.assertEqual(old[1]["memo"], "a", "replay must not mutate its input")


class CostShapeTest(unittest.TestCase):
    """What each strategy sends in a quiet steady-state cycle with nothing changed."""

    def test_nothing_changed_costs(self):
        expected_requests = {"FullRefresh": 1, "KeyHashSnapshot": 1, "BucketDiff/8": 1, "BucketDiff/64": 1,
                             "LocalSnapshot": 1}
        for name, make in FACTORIES.items():
            with self.subTest(strategy=name):
                source = Source(make_rows(500, seed=1))
                strat = make(source)
                strat.cycle()
                before = dict(source.meters)
                strat.cycle()
                self.assertEqual(source.meters["requests"] - before["requests"], expected_requests[name])

    def test_one_change_fetches_one_row_for_hashing_strategies(self):
        for name in ("KeyHashSnapshot", "BucketDiff/64", "LocalSnapshot"):
            with self.subTest(strategy=name):
                source = Source(make_rows(500, seed=1))
                strat = FACTORIES[name](source)
                strat.cycle()
                full = Source(make_rows(500, seed=1))
                fr = FullRefresh(full)
                fr.cycle()
                for s in (source, full):
                    s.update(17, memo="changed")
                b0, f0 = source.meters["bytes"], full.meters["bytes"]
                strat.cycle()
                fr.cycle()
                self.assertLess(source.meters["bytes"] - b0, (full.meters["bytes"] - f0) / 2)


if __name__ == "__main__":
    unittest.main()
