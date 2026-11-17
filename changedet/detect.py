"""One interface, four strategies, and webhook-style events.

Each strategy keeps its own cache and returns the events for one cycle. The cache derives every row and bucket hash
from the rows it holds and never stores a hash the agent returned, so a row changed between round
trips is caught on the next cycle.
"""
from .agent import HASH_BYTES, Agent, StatefulAgent, bucket_of, row_hash, xor_bytes
from .source import FIELDS, MODEL, QueueFull

ZERO = bytes(HASH_BYTES)
MAX_ATTEMPTS = 100_000


def diff_event(old, new):
    """The webhook payload for one row going old -> new (None = absent), or None if nothing changed."""
    if old is None and new is None:
        return None
    if old is None:
        return {"event": "created", "model": MODEL, "id": new["id"], "data": dict(new)}
    if new is None:
        return {"event": "deleted", "model": MODEL, "id": old["id"]}
    changed = {f: new[f] for f in FIELDS if old[f] != new[f]}
    if not changed:
        return None
    return {"event": "updated", "model": MODEL, "id": new["id"], "changed_fields": changed}


def replay(rows, events):
    """Apply events to a {id: row} map without mutating it; a webhook consumer would do exactly this."""
    out = {k: dict(v) for k, v in rows.items()}
    for e in events:
        if e["event"] == "created":
            out[e["id"]] = dict(e["data"])
        elif e["event"] == "updated":
            out[e["id"]].update(e["changed_fields"])
        elif e["event"] == "deleted":
            del out[e["id"]]
        else:
            raise ValueError(f"unknown event {e['event']!r}")
    return out


class Cache:
    def __init__(self, n_buckets=None):
        self.rows = {}
        self.hashes = {}
        self.n_buckets = n_buckets
        if n_buckets:
            self.buckets = [ZERO] * n_buckets
            self.members = [set() for _ in range(n_buckets)]

    def hash_of(self, key):
        return self.hashes.get(key)

    def put(self, row):
        key = row["id"]
        h = row_hash(row)  # derived from the row we hold, never taken from the agent
        old = self.hashes.get(key)
        if self.n_buckets:
            b = bucket_of(key, self.n_buckets)
            self.buckets[b] = xor_bytes(self.buckets[b], xor_bytes(old or ZERO, h))
            self.members[b].add(key)
        self.rows[key] = dict(row)
        self.hashes[key] = h

    def remove(self, key):
        h = self.hashes.pop(key)
        del self.rows[key]
        if self.n_buckets:
            b = bucket_of(key, self.n_buckets)
            self.buckets[b] = xor_bytes(self.buckets[b], h)
            self.members[b].discard(key)


class Strategy:
    name = "strategy"

    def __init__(self, source, n_buckets=None):
        self.source = source
        self.cache = Cache(n_buckets)
        self._events = []
        self.rows_fetched = 0  # full rows sent over the wire

    def _call(self, fn, *args):
        """On QueueFull: back off on the simulated clock and retry; never drop a request."""
        for _ in range(MAX_ATTEMPTS):
            try:
                return fn(*args)
            except QueueFull:
                self.source.clock.now += self.source.costs["backoff_ms"]
        raise RuntimeError("queue stayed full for MAX_ATTEMPTS retries")

    def _settle(self, key, new_row):
        ev = diff_event(self.cache.rows.get(key), new_row)
        if ev is None:
            return
        self._events.append(ev)
        if new_row is None:
            self.cache.remove(key)
        else:
            self.cache.put(new_row)

    def _fetch_and_settle(self, agent, keys):
        if not keys:
            return
        fetched = self._call(agent.rows, keys)
        self.rows_fetched += len(fetched)
        got = {r["id"]: r for r in fetched}
        for k in keys:
            self._settle(k, got.get(k))  # missing = deleted after the hash round trip

    def cycle(self):
        self._events = []
        self._run()
        return self._events

    def _run(self):
        raise NotImplementedError


class FullRefresh(Strategy):
    """Read every row and diff against the cache: today's all-or-nothing refresh and the test oracle."""
    name = "FullRefresh"

    def __init__(self, source):
        super().__init__(source)
        self.agent = Agent(source)

    def _run(self):
        rows = self._call(self.agent.all_rows)
        self.rows_fetched += len(rows)
        seen = set()
        for r in rows:
            seen.add(r["id"])
            self._settle(r["id"], r)
        for k in [k for k in self.cache.rows if k not in seen]:
            self._settle(k, None)


class KeyHashSnapshot(Strategy):
    """B: fetch (key, hash) for every row, then rows only for new or changed keys."""
    name = "KeyHashSnapshot"

    def __init__(self, source):
        super().__init__(source)
        self.agent = Agent(source)

    def _run(self):
        remote = self._call(self.agent.key_hashes, None)
        for k in [k for k in self.cache.rows if k not in remote]:
            self._settle(k, None)
        changed = [k for k, h in remote.items() if self.cache.hash_of(k) != h]
        self._fetch_and_settle(self.agent, changed)


class BucketDiff(Strategy):
    """A: bucket hashes; (key, hash) for changed buckets only; rows for new or changed keys."""

    def __init__(self, source, n_buckets=256):
        super().__init__(source, n_buckets)
        self.agent = Agent(source, n_buckets)
        self.name = f"BucketDiff/{n_buckets}"

    def _run(self):
        remote_buckets = self._call(self.agent.bucket_hashes)
        changed_b = [i for i, h in enumerate(remote_buckets) if h != self.cache.buckets[i]]
        if not changed_b:
            return
        remote = self._call(self.agent.key_hashes, changed_b)
        for b in changed_b:
            for k in [k for k in self.cache.members[b] if k not in remote]:
                self._settle(k, None)
        changed = [k for k, h in remote.items() if self.cache.hash_of(k) != h]
        self._fetch_and_settle(self.agent, changed)


class LocalSnapshot(Strategy):
    """Stateful agent returns changed rows plus deleted ids in one request (the honest baseline)."""
    name = "LocalSnapshot"

    def __init__(self, source):
        super().__init__(source)
        self.agent = StatefulAgent(source)

    def _run(self):
        res = self._call(self.agent.changes)
        self.rows_fetched += len(res["upserts"])
        for r in res["upserts"]:
            self._settle(r["id"], r)
        for k in res["deleted"]:
            self._settle(k, None)
