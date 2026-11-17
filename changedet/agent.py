"""The code that would run beside the customer's database.

Row hash: truncated SHA-256 over a canonical, type-tagged, length-prefixed encoding of the key and every field in
fixed order. The key is inside the hash so XOR-ing row hashes into a bucket cannot cancel a swap.
"""
import hashlib

from .source import COSTS, FIELDS

HASH_BYTES = COSTS["hash_bytes_on_wire"]


def _enc(v):
    if v is None:
        return b"n;"
    if isinstance(v, bool):
        raise TypeError("bool values are not part of the row model")
    if isinstance(v, int):
        return b"i" + str(v).encode() + b";"
    if isinstance(v, str):
        raw = v.encode("utf-8")
        return b"s" + str(len(raw)).encode() + b":" + raw
    raise TypeError(f"unsupported value type {type(v).__name__}")


def canonical(row):
    return _enc(row["id"]) + b"".join(_enc(row[f]) for f in FIELDS)


def row_hash(row):
    return hashlib.sha256(canonical(row)).digest()[:HASH_BYTES]


def bucket_of(key, n_buckets):
    return int.from_bytes(hashlib.sha256(_enc(key)).digest()[:8], "big") % n_buckets


def xor_bytes(a, b):
    return (int.from_bytes(a, "big") ^ int.from_bytes(b, "big")).to_bytes(HASH_BYTES, "big")


def bucket_hashes_of(rows, n_buckets):
    acc = [0] * n_buckets
    for r in rows:
        acc[bucket_of(r["id"], n_buckets)] ^= int.from_bytes(row_hash(r), "big")
    return [v.to_bytes(HASH_BYTES, "big") for v in acc]


class Agent:
    """Stateless agent: every call scans what it needs, goes through the serial queue, and is metered.

    It keeps no state between calls, so a reinstall or a second consumer costs nothing.
    """

    def __init__(self, source, n_buckets=None):
        self.source = source
        self.n_buckets = n_buckets

    def all_rows(self):
        def handler(conn):
            rows = self.source.all_rows()
            return rows, len(rows)
        return self.source.request(handler)

    def bucket_hashes(self):
        n = self.n_buckets

        def handler(conn):
            acc = [0] * n
            scanned = 0
            for r in self.source.iter_rows():
                acc[bucket_of(r["id"], n)] ^= int.from_bytes(row_hash(r), "big")
                scanned += 1
            return [v.to_bytes(HASH_BYTES, "big").hex() for v in acc], scanned
        return [bytes.fromhex(h) for h in self.source.request(handler)]

    def key_hashes(self, bucket_ids=None):
        """(key -> row hash) for every row, or only rows in bucket_ids. No bucket index: scans the table."""
        wanted = None if bucket_ids is None else set(bucket_ids)
        n = self.n_buckets

        def handler(conn):
            out = {}
            scanned = 0
            for r in self.source.iter_rows():
                scanned += 1
                if wanted is None or bucket_of(r["id"], n) in wanted:
                    out[str(r["id"])] = row_hash(r).hex()
            return out, scanned
        return {int(k): bytes.fromhex(v) for k, v in self.source.request(handler).items()}

    def rows(self, keys):
        keys = list(keys)

        def handler(conn):
            return self.source.get_many(keys), len(keys)  # primary-key lookups
        return self.source.request(handler)


class StatefulAgent:
    """LocalSnapshot's agent: keeps last cycle's hashes on the customer's machine.

    Tradeoff: state lives on the source machine, is lost on reinstall (next cycle is a full load), and serves one
    consumer, because sending a change advances the snapshot whether or not the consumer applied it.
    """

    def __init__(self, source):
        self.source = source
        self.snapshot = {}

    def changes(self):
        def handler(conn):
            current = {}
            upserts = []
            for r in self.source.iter_rows():
                h = row_hash(r)
                current[r["id"]] = h
                if self.snapshot.get(r["id"]) != h:
                    upserts.append(r)
            deleted = [k for k in self.snapshot if k not in current]
            self.snapshot = current
            return {"upserts": upserts, "deleted": deleted}, len(current)
        return self.source.request(handler)
