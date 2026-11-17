# change-detect

Incremental refresh for data sources that have no webhooks and no reliable updated-at column, behind a serial
request queue. A prototype on a mock source, not a fix for any real connector.

## The problem

Many construction ERPs (QuickBooks Desktop, Sage 100 Contractor, Sage 300 CRE) run on-prem, emit no change events,
and accept one request at a time with a short wait queue. A unified API that caches them has two bad options: refresh
everything on a fixed cadence, or miss third-party edits. Agave's engineering blog describes exactly this
([Engineering Challenges at Agave](https://useagave.com/blog/engineering-challenges-at-agave)):

> We population caches on a fixed cadence, and it’s an all-or-nothing action, meaning we have to fetch all data
> before we can determine what is actually new or different in order to update the cache.

The queue limits come from their public [rate-limits page](https://docs.agaveapi.com/agave-api/rate-limits):
these sources are processed serially with at most 5 queued requests.

## Assumptions

An agent can read rows on the source machine. The source has no usable updated-at field or change feed. Requests go
through a serial queue (one runs, at most 5 wait). Every cost assumption (latency, bandwidth, scan speed) lives in
one `COSTS` dict in `changedet/source.py` and is printed into `evidence/bench.json`.

## Four strategies, one interface

| Strategy | How it finds changes | Agent state |
|---|---|---|
| FullRefresh | read every row, diff against the cache (today's approach; the test oracle) | none |
| KeyHashSnapshot | agent returns (key, hash) for every row; fetch only rows whose hash differs | none |
| BucketDiff | agent returns XOR hashes for N buckets; drill into changed buckets, then fetch changed rows | none |
| LocalSnapshot | agent keeps last cycle's hashes on the source machine and returns only changed rows and deleted keys | yes |

All four emit webhook-style events (`created`, `updated`, `deleted`, carrying old and new values). The cache derives
row hashes from the rows it holds, never from hashes the agent returned, so a row edited between round trips still
converges.

## Results (100,000 rows, 5 cycles, seeded, identical mutation trace for every strategy)

Steady state at 0.1% of rows changed per cycle (`evidence/bench-table.txt`):

| Strategy | Bytes per cycle | Queue held |
|---|---:|---:|
| FullRefresh | 12,196,002 | 10,957 ms |
| KeyHashSnapshot | 4,298,627 | 4,840 ms |
| BucketDiff/256 | 1,364,658 | 3,692 ms |
| BucketDiff/4096 | 256,266 | 2,806 ms |
| LocalSnapshot | 9,934 | 1,208 ms |

Where the stateless options cross over (bracketed between tested rates of 0.1, 1, 10 and 50%):
- BucketDiff/4096 stops beating KeyHashSnapshot on bytes between 10% and 50%, and on queue time between 1% and 10%.
- BucketDiff/256 stops beating it on bytes between 1% and 10%, and on queue time between 0.1% and 1%.

Costs worth knowing:
- First sync is more expensive for the hash strategies: 16.5 to 16.6 MB against 12.2 MB for a full load.
- Nothing here reads zero rows: the agent still scans locally. The saving is bytes over the WAN and time holding the
  serial queue, which other clients are waiting on.
- A stateful agent (LocalSnapshot) wins by a wide margin but has to keep state on the customer's machine.

Every run's cache was checked against the source after a quiet cycle (`verified: true` for all). Under seeded
background load on the same queue, FullRefresh, KeyHashSnapshot, BucketDiff/256 and LocalSnapshot all hit
`QueueFull`, backed off, retried and converged (`background_run` in `bench.json`).

## What it promises

After a quiet cycle the cache equals the source. Several changes to one row between polls coalesce into one event.
Like any polling (full refresh included), a row that changes and changes back, or is created and deleted between
polls, produces no event. SHA-256 truncated to 128 bits on the wire; collisions are treated as negligible.

## Run

Python 3.10+, stdlib only.

```
python -m unittest discover -s tests -t .     # 54 tests
python tools/mutants.py                       # 7 broken variants; the suite must catch every one
python -m changedet.bench                     # full run (~5 min), writes evidence/bench.json; --small for a quick one
```

## Layout

- `changedet/source.py`: mock on-prem source (SQLite), serial queue with `QueueFull`, simulated clock, `COSTS`,
  seeded background load, seeded mutator and replayable trace.
- `changedet/agent.py`: row hash (truncated SHA-256 over key + fields, type-tagged), XOR bucket hashes, the stateless
  agent's metered calls, and the stateful agent used by LocalSnapshot.
- `changedet/detect.py`: the four strategies and the event payloads.
- `changedet/bench.py`: the benchmark.

## License

MIT
