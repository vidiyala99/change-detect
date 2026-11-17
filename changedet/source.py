"""The mock on-prem source.

A SQLite table of AP-invoice-like rows behind a serial request queue: one request runs at a time and at most
`max_waiting` may wait (Agave's rate-limits page for QuickBooks Desktop, Sage 100 Contractor, Sage 300 CRE). Time is a
simulated clock, so nothing sleeps and every run is deterministic.
"""
import json
import math
import random
import sqlite3

FIELDS = ("vendor", "job", "cost_code", "amount", "status", "memo")
MODEL = "ap_invoice"

# Every benchmark assumption lives here. bench.json and the evidence page print it verbatim.
COSTS = {
    "wire_encoding": "compact JSON, UTF-8; hashes as lowercase hex; bytes counted on the response body only",
    "hash_bytes_on_wire": 16,           # truncated SHA-256 (128 bits), sent as 32 hex chars
    "request_latency_ms": 200.0,        # fixed cost of one connector round trip to the on-prem agent
    "local_scan_ms_per_row": 0.01,      # agent reads + hashes one row on the customer's machine
    "wire_bytes_per_ms": 1250.0,        # 10 Mbit/s uplink from the customer's site
    "local_scan_holds_queue": True,     # the agent's scan runs inside the serial request it answers
    "max_waiting": 5,                   # requests allowed to wait behind the running one
    "backoff_ms": 500.0,                # client wait before retrying after QueueFull
}


class QueueFull(Exception):
    """The serial queue already has `max_waiting` requests waiting."""


class Clock:
    def __init__(self):
        self.now = 0.0


class BackgroundLoad:
    """Other clients' requests on the same serial queue.

    Either an explicit list of (arrival_ms, service_ms) for tests, or a seeded Poisson stream.
    """

    def __init__(self, arrivals=None, *, seed=None, mean_gap_ms=None, service_ms=None):
        self._fixed = sorted(arrivals or [], key=lambda a: a[0])
        self._i = 0
        self._rng = random.Random(seed) if seed is not None else None
        self._mean_gap = mean_gap_ms
        self._service = service_ms
        self._next_t = 0.0

    @classmethod
    def seeded(cls, seed, mean_gap_ms, service_ms):
        return cls(seed=seed, mean_gap_ms=mean_gap_ms, service_ms=service_ms)

    def arrivals_until(self, t):
        """Yield (arrival, service) in arrival order for every arrival at or before t, once each."""
        if self._rng is None:
            while self._i < len(self._fixed) and self._fixed[self._i][0] <= t:
                yield self._fixed[self._i]
                self._i += 1
            return
        while True:
            if self._next_t == 0.0 and self._i == 0:
                self._next_t = self._rng.expovariate(1.0 / self._mean_gap)
                self._i = 1
            if self._next_t > t:
                return
            # varied service times: fixed ones phase-lock with round poll intervals and hide the queue limit
            yield (self._next_t, self._service * self._rng.uniform(0.5, 1.5))
            self._next_t += self._rng.expovariate(1.0 / self._mean_gap)


VENDORS = ["Acme Concrete", "Bay Steel", "Delta Electric", "Granite Supply", "Hilltop Plumbing", "Kestrel HVAC",
           "Lumber Depot", "Pacific Glass"]
STATUSES = ["open", "approved", "paid", "void"]
MEMOS = ["", "pour", "rebar", "change order", "retainage", "net 30", None]


def _random_fields(rng):
    return {
        "vendor": rng.choice(VENDORS),
        "job": f"J-{rng.randint(100, 999)}",
        "cost_code": f"{rng.randint(1, 16):02d}-{rng.randint(100, 900)}",
        "amount": rng.randint(100, 5_000_000),
        "status": rng.choice(STATUSES),
        "memo": rng.choice(MEMOS),
    }


def make_rows(n, seed=0):
    rng = random.Random(seed)
    return [{"id": i, **_random_fields(rng)} for i in range(1, n + 1)]


def wire_bytes(payload):
    return len(json.dumps(payload, separators=(",", ":")).encode("utf-8"))


class Source:
    def __init__(self, rows=(), costs=COSTS, background=None):
        self.costs = costs
        self.clock = Clock()
        self.background = background
        self.after_request = []
        self.meters = {"requests": 0, "bytes": 0, "queue_held_ms": 0.0, "rows_scanned_local": 0, "queue_full": 0}
        self._jobs = []           # (start, end) of admitted jobs that may still be waiting or running
        self._server_free = 0.0
        self.conn = sqlite3.connect(":memory:")
        cols = ", ".join(f"{f}" for f in FIELDS)
        self.conn.execute(f"CREATE TABLE {MODEL} (id INTEGER PRIMARY KEY AUTOINCREMENT, {cols})")
        self.conn.executemany(
            f"INSERT INTO {MODEL} (id, {cols}) VALUES (?, {', '.join('?' for _ in FIELDS)})",
            [(r["id"], *(r[f] for f in FIELDS)) for r in rows])

    # ---- table (direct edits: the customer's own writes, not metered) ----
    def _row(self, t):
        return {"id": t[0], **dict(zip(FIELDS, t[1:]))}

    def all_rows(self):
        cur = self.conn.execute(f"SELECT id, {', '.join(FIELDS)} FROM {MODEL} ORDER BY id")
        return [self._row(t) for t in cur]

    def iter_rows(self):
        cur = self.conn.execute(f"SELECT id, {', '.join(FIELDS)} FROM {MODEL} ORDER BY id")
        for t in cur:
            yield self._row(t)

    def get(self, key):
        t = self.conn.execute(f"SELECT id, {', '.join(FIELDS)} FROM {MODEL} WHERE id = ?", (key,)).fetchone()
        return self._row(t) if t else None

    def get_many(self, keys):
        out = []
        keys = list(keys)
        for i in range(0, len(keys), 500):
            chunk = keys[i:i + 500]
            cur = self.conn.execute(
                f"SELECT id, {', '.join(FIELDS)} FROM {MODEL} WHERE id IN ({', '.join('?' for _ in chunk)})", chunk)
            out.extend(self._row(t) for t in cur)
        return out

    def count(self):
        return self.conn.execute(f"SELECT COUNT(*) FROM {MODEL}").fetchone()[0]

    def ids(self):
        return [t[0] for t in self.conn.execute(f"SELECT id FROM {MODEL} ORDER BY id")]

    def insert(self, fields, key=None):
        cols = ", ".join(FIELDS)
        if key is None:
            cur = self.conn.execute(f"INSERT INTO {MODEL} ({cols}) VALUES ({', '.join('?' for _ in FIELDS)})",
                                    [fields[f] for f in FIELDS])
            return cur.lastrowid
        self.conn.execute(f"INSERT INTO {MODEL} (id, {cols}) VALUES (?, {', '.join('?' for _ in FIELDS)})",
                          [key, *(fields[f] for f in FIELDS)])
        return key

    def update(self, key, **fields):
        bad = set(fields) - set(FIELDS)
        if bad:
            raise KeyError(f"unknown fields {sorted(bad)}")
        sets = ", ".join(f"{f} = ?" for f in fields)
        self.conn.execute(f"UPDATE {MODEL} SET {sets} WHERE id = ?", [*fields.values(), key])

    def delete(self, key):
        self.conn.execute(f"DELETE FROM {MODEL} WHERE id = ?", (key,))

    # ---- the serial queue ----
    def _waiting_at(self, t):
        self._jobs = [j for j in self._jobs if j[1] > t]
        return sum(1 for start, _ in self._jobs if start > t)

    def _admit(self, t, service):
        start = max(t, self._server_free)
        self._server_free = start + service
        self._jobs.append((start, self._server_free))
        return self._server_free

    def _drain_background(self, t):
        if self.background is None:
            return
        for arrival, service in self.background.arrivals_until(t):
            if self._waiting_at(arrival) < self.costs["max_waiting"]:
                self._admit(arrival, service)
            # else: the other client got QueueFull; it is not our concern

    def request(self, handler):
        """Run handler(conn) -> (payload, rows_scanned) as one serial request; return payload.

        Raises QueueFull (nothing runs) if max_waiting requests are already waiting.
        """
        t = self.clock.now
        self._drain_background(t)
        if self._waiting_at(t) >= self.costs["max_waiting"]:
            self.meters["queue_full"] += 1
            raise QueueFull()
        payload, scanned = handler(self.conn)
        nbytes = wire_bytes(payload)
        c = self.costs
        service = c["request_latency_ms"] + nbytes / c["wire_bytes_per_ms"]
        if c["local_scan_holds_queue"]:
            service += scanned * c["local_scan_ms_per_row"]
        self.clock.now = self._admit(t, service)
        m = self.meters
        m["requests"] += 1
        m["bytes"] += nbytes
        m["queue_held_ms"] += service
        m["rows_scanned_local"] += scanned
        for hook in list(self.after_request):
            hook(self)
        return payload


class Mutator:
    """Seeded creates (20%), updates (60%) and deletes (20%) at a given rate of the current row count."""

    def __init__(self, seed):
        self.rng = random.Random(seed)

    def step(self, source, rate, min_ops=1):
        if rate <= 0:
            return []
        live = source.ids()
        n = max(min_ops, math.ceil(rate * len(live)))
        ops = []
        for _ in range(n):
            roll = self.rng.random()
            if not live or roll < 0.2:
                fields = _random_fields(self.rng)
                key = source.insert(fields)
                live.append(key)
                ops.append(("create", {"id": key, **fields}))
            elif roll < 0.4:
                key = live.pop(self.rng.randrange(len(live)))
                source.delete(key)
                ops.append(("delete", key))
            else:
                key = live[self.rng.randrange(len(live))]
                new = _random_fields(self.rng)
                picked = self.rng.sample(FIELDS, self.rng.randint(1, 3))
                change = {f: new[f] for f in picked}
                source.update(key, **change)
                ops.append(("update", key, change))
        return ops


def apply_ops(source, ops):
    """Replay a recorded mutation trace (identical for every strategy)."""
    for op in ops:
        if op[0] == "create":
            row = op[1]
            source.insert({f: row[f] for f in FIELDS}, key=row["id"])
        elif op[0] == "update":
            source.update(op[1], **op[2])
        elif op[0] == "delete":
            source.delete(op[1])
        else:
            raise ValueError(f"unknown op {op[0]!r}")
