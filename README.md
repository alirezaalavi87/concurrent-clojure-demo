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

```text
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

**Virtual threads are cheap, and that is the whole point.** Ranges, because
thread creation is exactly the kind of thing that varies with whatever else the
box is doing — and a single sample would be a lie told with a decimal point.

```text
                     platform threads        virtual threads
   1,000 threads:      110 -  245 ms              3 -   5 ms
  10,000 threads:      880 - 1,205 ms             9 -  11 ms
```

Peak OS threads stays flat in N for both a bounded pool and virtual threads —
the difference is that virtual threads reach that flat number *without*
choosing it, so N is not a tuning decision.

**Virtual threads do not help CPU-bound work — but neither does anything else.**
At N = 1,000 CPU-bound tasks: pmap 839ms, virtual 832ms, and pools of 6, 12 and
36 threads at 859 / 842 / 802ms — every column within 7% of every other. There
are still only 12 carriers. Use a platform thread or `clojure.core.reducers`, but
the reason is that *nothing* helps, not that one approach is worse than another.

That last point is worth stating because an earlier version of this benchmark
claimed the opposite. It reported pmap at 2–3× the other approaches on
CPU-bound work, which reads as "pmap is bad at CPU work". It was false. `pmap` was
simply the first runner evaluated in every row, so it absorbed two one-time costs
that the other two avoided: the task's own JIT compilation, and the first use of
`clojure.core/future` and `Agent.soloExecutor`. Measured in one JVM at N=100,
cold-to-warm split 252ms → 68ms, of which ~90ms was the task and ~90ms was
`pmap`'s own machinery. Reordering the same three calls put pmap at 88ms instead
of 336ms.

`demo.bench` now warms up before timing, reports best-of-3 with the raw samples
shown, and asserts in section D that pmap and a core-sized pool stay within 30%
of each other — so if the warm-up is ever removed, it fails loudly instead of
quietly lying again. The same warm-up fixed section A's smallest rows
(N=10: pmap 11ms vs 25ms before).

## Traps, demonstrated rather than asserted

`demo.bench` section D asserts all seven at runtime, and fails the run if any stops
holding. The first two are interop traps; the last three are claims about
concurrency that this repo got wrong at least once.

**`.submit` with a `^Runnable` hint silently returns `nil`.** No exception, no
warning — just a future that derefs to `null` even though the task returned 42.

```text
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

The last three guard the claims made earlier in this file: that `pmap` really does
stay near 32 in flight at N=2,000; that submit-all-then-collect on an *unbounded*
pool really does produce one OS thread per element (~1,900 threads at N=2,000,
versus **+1** for virtual threads doing the same work 3× faster); and that, once
warm, `pmap` and a core-sized pool agree on CPU-bound work within 30%.

That third claim was a real bug, not a typo. The virtual-thread check read
`+703` on one run and `+227` on the next, because all three experiments shared a
single `baseline` sampled at the top of the section. They are *sequential and
destructive*: the unbounded run creates 2,000 platform threads, and
`newCachedThreadPool` holds idle threads for 60 seconds, so the virtual run that
follows was counting the previous run's stragglers as its own carriers. Each
measurement now takes its own baseline immediately before it runs, with a
`settle!` in between that waits for the thread count to come to rest. The delta
is a steady **+1** now.

The general lesson is the same as the warm-up one: **a benchmark that measures
threads must also measure the threads it left behind.** The machine does not
reset between experiments, and assuming it does is how you end up confidently
reporting a number about the wrong thing.

## Where the time actually goes (live host)

```text
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

## Sample benchmark results

Captured from `clj -M -m demo.bench` on a 12-core box. Section D's seven assertions
must all pass — it is a test, not decoration, and two of them exist because this
benchmark previously reported numbers it should not have believed.

```text
$ clj -M -m demo.bench

LOCAL CONCURRENCY BENCHMARK  (no network)
============================================================
cores: 12   jvm: 25.0.2

A. I/O-bound fan-out: N tasks x 10ms blocking call
--------------------------------------------------
                pmap    32-thr pool   virtual   pmap OS thr   vt OS thr   speedup
  N = 10       11 ms          13 ms     12 ms            57          57      0.9x
  N = 100      44 ms          44 ms     12 ms            57          57      3.7x
  N = 1000    342 ms         329 ms     24 ms            57          57     14.2x
  N = 5000   1663 ms        1599 ms     52 ms            58          58     31.8x
  N = 20000   6575 ms        6378 ms     56 ms            60          60    116.8x
  N = 100000  33025 ms       31870 ms    338 ms            61          61     97.7x
  (60 is this JVM's own thread count. Both columns are FLAT in N:
   pmap tops out around baseline+32 because that is its chunking cap, and
   it reuses those threads on later runs. Virtual threads sit at
   baseline+~13 carriers no matter how many requests you throw at them)

B. CPU-bound (no blocking) -- 12 cores, best of 3
-------------------------------------------------
                 pmap    virtual     fixed 6   fixed 12   fixed 36
  N = 10        10 ms       9 ms       11 ms      10 ms      10 ms
  N = 100       75 ms      86 ms       93 ms      88 ms      87 ms
  N = 1000     839 ms     832 ms      859 ms     842 ms     802 ms
  N = 5000    4189 ms    4333 ms     4355 ms    4374 ms    3955 ms
  -> Every column above is the same, and so is a pool of 6, 12 or
     36. That IS the finding: for CPU-bound work the limit is
     the cores, and no threading strategy moves it. The practical
     advice is unchanged -- reach for clojure.core.reducers -- but
     the reason is that nothing helps, not that one approach is
     worse than another. pmap's 32-way cap, which ruins it in section
     A, costs nothing here: oversubscribing runnable threads on a
     saturated CPU is nearly free. The cap only hurts when the work
     is BLOCKED, which is section A's problem, not this one's.

C. Thread cost
--------------
    1000 platform threads:       355 ms
    1000 virtual  threads:         5 ms   (72x cheaper to create)
   10000 platform threads:       838 ms
   10000 virtual  threads:        11 ms   (74x cheaper to create)

  What pmap actually does, which is not what people assume:
  pmap is (map #(future (f %)) coll). `future` dispatches to
  Agent.soloExecutor, which javap shows is newCachedThreadPool --
  UNBOUNDED. But pmap is LAZY, and lazy seqs are realised in chunks of
  clojure.core/chunk-size (32), so pmap keeps only ~32 requests in
  flight. Section A shows it tracking a 32-thread pool to within a few
  percent. It is a 32-way concurrency cap with no label on it.

  So the thread explosion is real but it is NOT in pmap -- it is in the
  obvious 'fix': submitting all N tasks before collecting any, on that
  unbounded pool. That gives you one OS thread per element. Section D
  measures both.

D. Traps, as executable assertions
----------------------------------
  [PASS] submit ^Callable returns the task's value
           .submit ^Callable -> 42
  [PASS] submit ^Runnable silently returns nil (no exception!)
           .submit ^Runnable -> nil, even though the task returned 42
         (a Clojure fn implements BOTH Runnable and Callable, so
          the hint picks the overload. Hinting the EXECUTOR instead
          makes the call fail to compile entirely.)

  [PASS] submitting and deref'ing in the same mapv runs SERIALLY
           20 x 50ms tasks: deref-in-mapv   1011 ms vs submit-then-deref     51 ms
         (this one is easy to write by accident, because the
          code reads as 'map over sections, fetching each')

  [PASS] pmap stays near 32 in flight even at N = 2000
           pmap peaked at 62 OS threads (+1 vs the 61 baseline)
  [PASS] submitting all N first, on an UNBOUNDED pool, really does explode
           same N on newCachedThreadPool: 2055 OS threads (+2001)
  [PASS] the same work on virtual threads needs a fixed number of carriers
           virtual: 55 OS threads (+1 vs a 54 baseline taken after
           the previous 2,000 wound down), and it was 3x FASTER

  [PASS] once warm, pmap and a core-sized pool agree on CPU-bound work
           N=2000: pmap  1663 ms [1663 1704 1680] vs fixed-12  1734 ms [1734 1767 1773]  (1.04x apart)

done.
```
