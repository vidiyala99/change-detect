"""Benchmark: every strategy replays the identical seeded snapshot and mutation trace.

    python -m changedet.bench                 # full run: 100,000 rows, writes evidence/bench.json
    python -m changedet.bench --small out.json

The bootstrap (first full load) is reported apart; steady-state cycles are the headline. Where BucketDiff (A) stops
beating KeyHashSnapshot (B) is reported as a bracket between tested rates, never as a single point.
"""
import argparse
import hashlib
import json
import os
import sys

from .detect import BucketDiff, FullRefresh, KeyHashSnapshot, LocalSnapshot
from .source import COSTS, BackgroundLoad, Mutator, Source, apply_ops, make_rows

FULL = dict(n_rows=100_000, rates=[0.001, 0.01, 0.1, 0.5], bucket_counts=[256, 4096], cycles=5, seed=1)
SMALL = dict(n_rows=2_000, rates=[0.001, 0.01, 0.1, 0.5], bucket_counts=[16, 256], cycles=3, seed=1)
POLL_INTERVAL_MS = 60_000.0
BACKGROUND = dict(mean_gap_ms=100.0, service_ms=250.0)
METERS = ("requests", "bytes", "queue_held_ms", "rows_scanned_local", "queue_full")


def _pct(r):
    return f"{r * 100:g}%"


def crossover_bracket(rates, a_vals, b_vals):
    """Where A (lower is better) stops beating B, as a bracket between tested rates."""
    wins = [a < b for a, b in zip(a_vals, b_vals)]
    a_wins_at = [r for r, w in zip(rates, wins) if w]
    monotonic = all(wins[i] or not wins[i + 1] for i in range(len(wins) - 1))
    bracket = None
    if not a_wins_at:
        summary = f"A beats B at no tested rate ({_pct(rates[0])} to {_pct(rates[-1])})"
    elif all(wins):
        summary = f"A beats B at every tested rate, up to {_pct(rates[-1])}"
    else:
        i = wins.index(False)
        bracket = [rates[i - 1], rates[i]] if i > 0 else None
        if bracket:
            summary = f"A stops beating B between {_pct(bracket[0])} and {_pct(bracket[1])}"
        else:
            summary = f"A loses at {_pct(rates[0])}"
        if not monotonic:
            summary += "; not monotonic, A also wins at " + ", ".join(_pct(r) for r in a_wins_at)
    return {"bracket": bracket, "a_wins_at": a_wins_at, "monotonic": monotonic, "summary": summary}


def _state_digest(source):
    h = hashlib.sha256()
    for r in source.iter_rows():
        h.update(json.dumps(r, sort_keys=True, separators=(",", ":")).encode())
    return h.hexdigest()[:16]


def _make_trace(n_rows, rate, cycles, seed):
    ref = Source(make_rows(n_rows, seed))
    mut = Mutator(seed=seed * 1_000_003 + round(rate * 1_000_000))
    return [mut.step(ref, rate) for _ in range(cycles)]


def _measure(strat, source, fn):
    before = dict(source.meters)
    t0 = source.clock.now
    f0 = strat.rows_fetched
    fn()
    out = {k: source.meters[k] - before[k] for k in METERS}
    out["elapsed_ms"] = source.clock.now - t0
    out["rows_fetched"] = strat.rows_fetched - f0
    return out


def _run_one(make_strategy, n_rows, seed, trace, background=None):
    source = Source(make_rows(n_rows, seed), background=background)
    strat = make_strategy(source)
    digests = [_state_digest(source)]
    bootstrap = _measure(strat, source, strat.cycle)
    verified = len(strat.cache.rows) == source.count()
    steady = []
    for ops in trace:
        apply_ops(source, ops)
        digests.append(_state_digest(source))
        source.clock.now += POLL_INTERVAL_MS
        steady.append(_measure(strat, source, strat.cycle))
        verified = verified and len(strat.cache.rows) == source.count()
    # full check once at the end (cheap checks above every cycle)
    verified = verified and strat.cache.rows == {r["id"]: r for r in source.all_rows()}
    mean = {k: sum(c[k] for c in steady) / len(steady) for k in steady[0]} if steady else {}
    mean["cycles"] = len(steady)
    return strat.name, {"bootstrap": bootstrap, "steady": mean, "verified": verified}, digests


def _factories(bucket_counts):
    fs = [("FullRefresh", FullRefresh), ("KeyHashSnapshot", KeyHashSnapshot)]
    fs += [(f"BucketDiff/{b}", (lambda s, b=b: BucketDiff(s, n_buckets=b))) for b in bucket_counts]
    fs.append(("LocalSnapshot", LocalSnapshot))
    return fs


def run(n_rows, rates, bucket_counts, cycles, seed, background_rate=0.01, log=None):
    results, trace_digests = [], {}
    for rate in rates:
        trace = _make_trace(n_rows, rate, cycles, seed)
        trace_digests[str(rate)] = {}
        for name, make in _factories(bucket_counts):
            if log:
                log(f"rate {_pct(rate)} {name}")
            name, res, digests = _run_one(make, n_rows, seed, trace)
            trace_digests[str(rate)][name] = digests
            results.append({"rate": rate, "strategy": name, **res})

    def steady(name, rate, metric):
        return next(r["steady"][metric] for r in results if r["strategy"] == name and r["rate"] == rate)

    crossover = {}
    for b in bucket_counts:
        a = f"BucketDiff/{b}"
        crossover[a] = {m: crossover_bracket(rates, [steady(a, r, m) for r in rates],
                                             [steady("KeyHashSnapshot", r, m) for r in rates])
                        for m in ("bytes", "queue_held_ms")}

    bg_results = []
    trace = _make_trace(n_rows, background_rate, cycles, seed)
    for name, make in _factories(bucket_counts[:1]):
        if log:
            log(f"background load {name}")
        bg = BackgroundLoad.seeded(seed, **BACKGROUND)
        name, res, _ = _run_one(make, n_rows, seed, trace, background=bg)
        total_full = res["bootstrap"]["queue_full"] + res["steady"]["queue_full"] * res["steady"]["cycles"]
        bg_results.append({"strategy": name, "queue_full": round(total_full), **res})

    return {
        "costs": COSTS,
        "config": {"n_rows": n_rows, "rates": rates, "bucket_counts": bucket_counts, "cycles": cycles,
                   "seed": seed, "poll_interval_ms": POLL_INTERVAL_MS},
        "results": results,
        "crossover": crossover,
        "background_run": {"rate": background_rate, "load": BACKGROUND, "results": bg_results},
        "trace_digests": trace_digests,
    }


def write(result, path):
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2)
        f.write("\n")


def table(result):
    lines = ["COSTS " + json.dumps(result["costs"]), "",
             f"{'rate':>6} {'strategy':<16} {'requests':>8} {'bytes':>12} {'queue ms':>10} {'scanned':>9}"]
    for r in result["results"]:
        s = r["steady"]
        lines.append(f"{_pct(r['rate']):>6} {r['strategy']:<16} {s['requests']:>8.1f} {s['bytes']:>12,.0f} "
                     f"{s['queue_held_ms']:>10,.0f} {s['rows_scanned_local']:>9,.0f}")
    lines.append("")
    for a, m in result["crossover"].items():
        for metric, c in m.items():
            lines.append(f"{a} vs KeyHashSnapshot, {metric}: {c['summary']}")
    return "\n".join(lines)


def main(argv=None):
    p = argparse.ArgumentParser()
    p.add_argument("out", nargs="?", default=os.path.join(os.path.dirname(__file__), "..", "evidence",
                                                          "bench.json"))
    p.add_argument("--small", action="store_true")
    args = p.parse_args(argv)
    result = run(**(SMALL if args.small else FULL), log=lambda m: print(m, file=sys.stderr, flush=True))
    write(result, args.out)
    print(table(result))


if __name__ == "__main__":
    main()
