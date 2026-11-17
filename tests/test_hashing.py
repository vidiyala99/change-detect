"""row hash over key + fields, XOR bucket hashes."""
import unittest

from changedet.agent import bucket_hashes_of, bucket_of, row_hash, xor_bytes
from changedet.source import FIELDS


def row(id_, **over):
    base = {"id": id_, "vendor": "Acme", "job": "J-1", "cost_code": "03-100",
            "amount": 125000, "status": "open", "memo": "pour"}
    base.update(over)
    return base


class RowHashTest(unittest.TestCase):
    def test_stable_across_calls_and_dict_order(self):
        r = row(1)
        reordered = {k: r[k] for k in reversed(list(r))}
        self.assertEqual(row_hash(r), row_hash(row(1)))
        self.assertEqual(row_hash(r), row_hash(reordered))

    def test_every_field_changes_the_hash(self):
        base = row_hash(row(1))
        for f in FIELDS:
            with self.subTest(field=f):
                self.assertNotEqual(base, row_hash(row(1, **{f: None})))

    def test_key_changes_the_hash(self):
        self.assertNotEqual(row_hash(row(1)), row_hash(row(2)))

    def test_type_tags_distinguish_int_string_and_null(self):
        self.assertNotEqual(row_hash(row(1, amount=5)), row_hash(row(1, amount="5")))
        self.assertNotEqual(row_hash(row(1, memo="")), row_hash(row(1, memo=None)))
        self.assertNotEqual(row_hash(row(1, memo="None")), row_hash(row(1, memo=None)))

    def test_field_boundaries_are_unambiguous(self):
        a = row(1, vendor="ab", job="c")
        b = row(1, vendor="a", job="bc")
        self.assertNotEqual(row_hash(a), row_hash(b))

    def test_hash_width_is_16_bytes(self):
        self.assertEqual(len(row_hash(row(1))), 16)


class BucketTest(unittest.TestCase):
    def test_bucket_is_deterministic_and_in_range(self):
        for n in (1, 7, 256, 4096):
            for k in range(200):
                b = bucket_of(k, n)
                self.assertEqual(b, bucket_of(k, n))
                self.assertTrue(0 <= b < n)

    def test_buckets_spread_keys(self):
        used = {bucket_of(k, 256) for k in range(5000)}
        self.assertGreater(len(used), 240)

    def test_bucket_hash_is_xor_of_row_hashes_order_free(self):
        rows = [row(k, amount=k) for k in range(50)]
        a = bucket_hashes_of(rows, 8)
        b = bucket_hashes_of(list(reversed(rows)), 8)
        self.assertEqual(a, b)
        self.assertEqual(len(a), 8)
        self.assertEqual(bucket_hashes_of([], 4), [bytes(16)] * 4)

    def test_in_place_update_equals_recompute(self):
        rows = {k: row(k, amount=k) for k in range(100)}
        hashes = bucket_hashes_of(rows.values(), 16)
        # update, insert, delete applied by XOR-ing hashes in and out
        old, new = rows[5], row(5, amount=999)
        b = bucket_of(5, 16)
        hashes[b] = xor_bytes(hashes[b], xor_bytes(row_hash(old), row_hash(new)))
        rows[5] = new
        ins = row(1000)
        b = bucket_of(1000, 16)
        hashes[b] = xor_bytes(hashes[b], row_hash(ins))
        rows[1000] = ins
        gone = rows.pop(7)
        b = bucket_of(7, 16)
        hashes[b] = xor_bytes(hashes[b], row_hash(gone))
        self.assertEqual(hashes, bucket_hashes_of(rows.values(), 16))

    def test_swapping_values_of_two_rows_in_one_bucket_changes_bucket_hash(self):
        # XOR is order-free, so without the key in the hash this swap is invisible.
        n = 4
        keys = [k for k in range(100) if bucket_of(k, n) == 0][:2]
        k1, k2 = keys
        before = [row(k1, vendor="A", amount=1), row(k2, vendor="B", amount=2)]
        after = [row(k1, vendor="B", amount=2), row(k2, vendor="A", amount=1)]
        self.assertNotEqual(bucket_hashes_of(before, n)[0], bucket_hashes_of(after, n)[0])

    def test_two_identical_rows_do_not_cancel(self):
        n = 4
        k1, k2 = [k for k in range(100) if bucket_of(k, n) == 0][:2]
        same = dict(vendor="X", amount=1)
        self.assertNotEqual(bucket_hashes_of([row(k1, **same), row(k2, **same)], n)[0], bytes(16))


if __name__ == "__main__":
    unittest.main()
