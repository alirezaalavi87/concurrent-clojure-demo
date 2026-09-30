# concurrent-clj

Six ways to do the same fan-out in Clojure, with the trade-offs written down
next to the code and the numbers measured rather than asserted.

Every approach fetches the same sections from
`jsonplaceholder.typicode.com` through the same signature:

```clojure
(fetch-all sections)        ;; => [{:section :posts :body {...}} ...]
(fetch-all sections opts)   ;; opts: {:max-in-flight 8 :timeout-ms 3000}
```

They all return the same shape, **in input order, and never throw** — a failed
or timed-out fetch comes back as `{:section _ :error "..."}` or
`{:section _ :status :timeout}`. That uniformity is the point: once every
approach degrades identically, a benchmark measures concurrency and nothing
else.

## Run it

```bash
clojure -M -m demo.core        # all six, live, 12 requests
clojure -M -m demo.bench       # deterministic, NO network (~90s at full scale)
clojure -M -m demo.bench 5000  # ... but stop the fan-out at 5,000
clojure -M -m demo.bench-net   # live, budgeted, explains where time goes
```

Needs JDK 21+ (virtual threads, `StructuredTaskScope`); developed on JDK 25.

| file | what it holds |
| --- | --- |
| `src/demo/core.clj` | the six approaches, each with benefits, drawbacks and its own footgun |
| `src/demo/bench.clj` | local benchmark — no network, so it is reproducible |
| `src/demo/bench_net.clj` | live benchmark against the real API, under a hard request budget |
| `main.clj` | the original scratch file this grew out of; superseded, kept for reference |

## The approaches

1. **Sequential** — no concurrency to reason about, and the baseline everything
   else is measured against.
2. **`pmap`** — four tokens, preserves order, and the right answer below 32
   requests.
3. **Bounded OS thread pool** — when you need a *stated* concurrency ceiling and
   have to run on an old JVM.
4. **Virtual threads** *(recommended default for I/O fan-out)* — you keep
   straight-line blocking code, and thread cost stops being a design constraint.
5. **`core.async/io-thread`** — the channel version, if you already want
   core.async or need channel composition.
6. **`StructuredTaskScope`** — fail-fast and guaranteed cleanup; the only one of
   the six that cannot leak a task.

## What the measurements actually said

**`pmap` has an unlabelled 32-way concurrency cap.** It is `(map #(future (f %)) coll)`,
and it is *lazy*, so it only ever has about 32 requests in flight. Its runtime
tracks a deliberately 32-thread pool to within a few percent, and tracks
`N/32 × latency` to within a few percent:

```
N tasks      pmap    32-thr pool    unbounded pool    virtual threads
   1,000    348ms            333ms              73ms             39ms
   3,200  1,052ms          1,034ms             135ms             81ms
```

It is **not** slow because threads are expensive — pre-creating 200 idle OS
threads does not change its runtime at all. If 32 in flight is enough, `pmap` is
fine. If you need 500 requests at a server that can take them, `pmap` will not
get you there and will not tell you why. It also cannot be given a
`:max-in-flight`: measured against a ceiling of 4, `pmap` ran 6 at once while
all five other approaches stayed at 4.

**The thread explosion is real, but it is not in `pmap`.** It is in the obvious
"fix" — submitting all N tasks before collecting any, on an *unbounded* pool.
`future` dispatches to `Agent.soloExecutor`, which `javap` shows is
`newCachedThreadPool`. That idiom gives one OS thread per element: 2,000 tasks
peaked at 2,001 extra platform threads, and virtual threads did the same work
3× faster using 1.

**Virtual threads are cheap, and that is the whole point.**

```
1,000 platform threads:   172 ms        1,000 virtual threads:   4 ms
10,000 platform threads: 1,205 ms       10,000 virtual threads:   9 ms
```

Peak OS threads stays flat in N for both a bounded pool and virtual threads —
the difference is that virtual threads reach that flat number *without*
choosing it, so N is not a tuning decision.

**Virtual threads do not help CPU-bound work.** At N = 1,000 CPU-bound tasks:
pmap 872ms, virtual 864ms, fixed 12-thread pool 888ms. There are still only 12
carriers. Use a platform thread or `clojure.core.reducers`.

## Two interop traps, demonstrated rather than asserted

`demo.bench` section D asserts both at runtime.

**`.submit` with a `^Runnable` hint silently returns `nil`.** No exception, no
warning — just a future that derefs to `null` even though the task returned 42.

```
.submit ^Callable (fn [] 42)  ->  42
.submit ^Runnable (fn [] 42)  ->  nil
```

A Clojure `fn` implements both interfaces, so the hint picks the overload.
Hint the *executor* instead (`^ExecutorService`) and it fails to compile
outright: `More than one matching method found: submit`. `clojure.core/future`
gets this right (`^Callable`); hand-rolled `.submit` is where the trap lives.
`core/submit-task` exists to make the right shape the default.

**Deref'ing inside the same `mapv` that submits is sequential.** 20 × 50ms
tasks: 1,011ms that way, 51ms submit-all-then-collect. The code reads like
"map over sections, fetching each" and is parallel in exactly the same way.
This one is not hypothetical — the first version of `pmap-fetch` in this repo
had it, and looked fine right up until a peak-concurrency meter read `1`.

## Where the time actually goes (live host)

```
url            connect   headers      body    bytes
/users/1        1039 ms     291 ms       7 ms      509
/users/2           1 ms     283 ms       1 ms      509
```

The handshake is a **one-off** ~1s; after that the per-request cost is ~280ms of
*server* think time, and the body is ~2ms. So:

- **Pool your connections first.** Three requests cost 1,464ms cold and 866ms
  warm on a pooled client. That is a bigger win than any threading change, and
  it is orthogonal to concurrency.
- **Then overlap the 280ms.** That is what the threads are for.
- Two beliefs worth discarding: that "`slurp` pays a handshake per call" (it
  does not — the JVM's `HttpURLConnection` keep-alive cache reuses connections;
  connect drops to 0ms on the second call), and that the ~2s six-request
  baseline is a connection-setup cost. It is the server thinking.

## Why the live benchmark refuses to rank the approaches

`demo.bench-net` measures against a live API on purpose, and still will not tell
you which approach is faster, because it cannot:

- The same `pmap` code has been observed at 771ms on one pass and 1,368ms on the
  next; the virtual-thread version degraded 2,029ms → 4,124ms across rounds.
- Six sections is far too small a sample.

What it *is* good for is the shape: the connect/headers/body split, and the
connection-reuse table. Both are stable. Relative speed comes from
`demo.bench`, which touches no network and gives the same answer every time.

It is also polite by construction, because hammering a free public demo API is a
small denial-of-service tool:

1. a **hard request budget** claimed from an atomic counter *before* the work
   starts, so an overshoot is refused rather than sent and regretted;
2. a **concurrency ceiling** passed in as `:max-in-flight` and enforced by each
   approach — and the benchmark *measures* the resulting peak rather than
   trusting it;
3. an **inter-round delay** so approaches do not arrive as a burst.

The budget is printed on exit, including from a shutdown hook, so an aborted run
still reports what it spent. Default run: 51 of 60.

## Practices that matter with virtual threads

- **Do not pool them.** Make one per task; the whole point is that they are
  cheap. Pool *concurrency* with a `Semaphore`, not threads —
  `(Semaphore. n true)`, always, because an unfair semaphore lets a steady stream
  of new arrivals starve older waiters.
- **Do not share mutable state.** Virtual threads are not preemptible, so
  `swap!`/`alter` and friends can pin a carrier. Submit everything, then collect
  everything.
- **Avoid `synchronized` inside a virtual thread.** It pins the carrier. Use a
  `ReentrantLock` if you need mutual exclusion.
- **A timeout does not cancel.** `Future.get` timing out leaves the task
  running, still holding its permit. The real timeouts belong on the HTTP client.
- **`core.async`: use `io-thread`, not `go`.** `go` blocks are not virtual
  threads. Also note `<!!` is a plain function in current core.async with no
  timeout arity — race against `(async/timeout msecs)` with `alts!!` instead.
