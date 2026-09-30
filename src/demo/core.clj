(ns demo.core
  "Six ways to fetch the same six URLs, in increasing order of sophistication.

  Every approach has the same signature and the same return shape, so you can
  swap one for another in a benchmark without touching the call site:

      (fetch-all sections)          ;; => [{:section :posts :body {...}} ...]
      (fetch-all sections opts)     ;; opts: {:max-in-flight 8 :timeout-ms 3000}

  The return value is a vector of maps, one per requested section, ALWAYS in
  the same order as the input and NEVER throwing: a failed or timed-out fetch
  comes back as {:section _ :error \"...\"} or {:section _ :status :timeout}.
  That uniformity is the point. Once every approach degrades the same way, the
  benchmark measures concurrency and nothing else.

  Read the comment block above each function. Each one says what the approach
  is good for, what it costs, and which footgun is specific to it."
  (:require [cheshire.core :as json]
            [clj-http.client :as http]
            [clojure.core.async :as async]
            [clojure.string :as string])
  (:import (java.util.concurrent Callable ExecutorService ExecutionException
                                 Executors Future Semaphore StructuredTaskScope
                                 StructuredTaskScope$Subtask ThreadLocalRandom
                                 TimeUnit TimeoutException)
           (org.apache.http.impl.conn PoolingHttpClientConnectionManager)))

;; ---------------------------------------------------------------------------
;; The thing being fetched
;; ---------------------------------------------------------------------------

(def sections
  {:posts "posts" :comments "comments" :albums "albums"
   :photos "photos" :todos "todos" :users "users"})

(def base-url "https://jsonplaceholder.typicode.com")

(defn section-url
  "URL for item `i` of a section, e.g. (:posts sections) -> \"posts\".

  `name` accepts either a keyword or a string, so callers can pass whichever
  they have without converting first."
  [section i]
  (format "%s/%s/%s" base-url (name section) i))

(def default-opts
  "Defaults for every approach.

  :max-in-flight  caps how many requests hit the server at once. This is the
                  one limit you almost always want. Without it, an unbounded
                  fan-out against a real API is a self-inflicted denial of
                  service -- the kind you were worried about inflicting on
                  jsonplaceholder, but aimed at your own vendor instead.

  :timeout-ms     caps how long a single request may take. Note this bounds
                  DURATION, not concurrency; a semaphore bounds concurrency.
                  You want both, because a hung request holds its permit
                  forever if you only have the semaphore."
  {:max-in-flight 8
   :timeout-ms    3000})

;; ---------------------------------------------------------------------------
;; The two fetch primitives
;; ---------------------------------------------------------------------------

(defn slurp-fetch
  "The naive fetch: `slurp` a URL, nothing else.

  BENEFIT: zero configuration, and it is the shortest possible illustration of
  what the profile actually looks like.

  DRAWBACK: it gives you a String rather than data, no status code, no headers
  and no timeouts, so a hanging server wedges the calling thread forever.

  CORRECTING A COMMON ASSUMPTION: the popular claim is that `slurp` opens a
  brand-new connection every call and so pays a full handshake each time. On a
  HotSpot JVM that is NOT true -- `HttpURLConnection` has a built-in
  keep-alive cache (`http.keepAlive`, 5 connections by default), so repeated
  calls to the same host reuse the connection. Measured here: the first
  request spent 1039ms in `.connect`, the next two spent 1ms and 0ms, and three
  `slurp` calls cost 827ms and then 851ms. So the handshake is paid ONCE per
  connection, not once per request.

  That does not make `slurp` as good as a pooled client -- 5 connections is a
  very small cache, and the cache is the JVM's, not yours -- but the reason it
  was slow was never 'a handshake per call'. See http-fetch for the real
  breakdown."
  [section i]
  (json/parse-string (slurp (section-url section i)) true))

(defonce ^:private connection-manager
  ;; A real client keeps connections ALIVE and reuses them. This single change
  ;; usually beats any threading change you make.
  ;;
  ;; max-total        -> across all routes to this host
  ;; default-max-per-route -> to any one host, which is what protects the target
  ;;
  ;; Lifecycle: this is a Closeable and lives for the life of the process. That
  ;; is fine for a long-running server and wrong for a short script -- a
  ;; with-open around the client would be more correct in a library.
  (doto (PoolingHttpClientConnectionManager.)
    (.setMaxTotal 20)
    (.setDefaultMaxPerRoute 8)))

(defn http-fetch
  "The practical fetch: pooled connections, real timeouts, parsed JSON.

  BENEFIT: connection reuse under YOUR control rather than the JVM's 5-entry
  default; explicit connect/socket timeouts so a dead server cannot wedge a
  thread forever; a status code you can check; and a parsed body.

  Measured breakdown of ONE request to this host, in three phases:

        .connect (DNS+TCP+TLS)   1039ms on the first call, 1ms and 0ms after
        response headers         ~280ms every time
        response body            ~2ms

  So the expensive part is the server's ~280ms of think time, and the ~1s
  handshake is a one-off per connection. Two consequences, both worth more than
  any threading change: pool the connections, and overlap the 280ms.

  DRAWBACK: more moving parts, and you now own the connection manager's
  lifecycle. Also, `max-per-route 8` is a ceiling the server may still see as
  too aggressive -- it is a starting point to tune, not a safe default for
  somebody else's free API."
  [section i]
  (let [{:keys [status body]}
        (http/get (section-url section i)
                  {:connection-manager  connection-manager
                   :connect-timeout     2000
                   :socket-timeout      3000
                   :throw-exceptions    false})]
    (if (= 200 status)
      (json/parse-string body true)
      (throw (ex-info "non-200 response" {:section section :status status})))))

;; ---------------------------------------------------------------------------
;; Small interop helpers
;;
;; These exist mainly so the demo can point at them. Both wrap genuine
;; footguns that are easy to hit when talking to java.util.concurrent from
;; Clojure.
;; ---------------------------------------------------------------------------

(defn ^Future submit-task
  "Submit `f` to `ex` AS A CALLABLE, so that `.get` returns the task's value.

  THE HINT IS LOAD-BEARING. A Clojure function implements BOTH `Runnable` and
  `Callable` (they both extend Object, and clojure.lang.AFn implements them),
  and `ExecutorService.submit` is overloaded on both. So you get one of three
  outcomes depending on what hints are in play:

      (.submit ex #(f))                 ;; untyped executor -> reflective call
                                        ;;   -> works, returns the value
      (.submit ^ExecutorService ex #(f));; once you hint the executor (good
                                        ;;   practice!) this now FAILS TO
                                        ;;   COMPILE: \"More than one matching
                                        ;;   method found: submit\"
      (.submit ex ^Runnable (fn [] 42)) ;; explicitly Runnable -> SILENTLY
                                        ;;   returns nil from .get, because
                                        ;;   submit(Runnable) wraps via
                                        ;;   Executors/callable whose call()
                                        ;;   returns null
      (.submit ex ^Callable (fn [] 42)) ;; explicitly Callable -> 42. Correct.

  That third case is the dangerous one: no exception, no warning, just a
  future that derefs to nil. Note that clojure.core/future gets this right --
  core.clj does `.submit Agent/soloExecutor ^Callable f` -- so `future` is
  fine and hand-rolled `.submit` is where the trap lives."
  [^ExecutorService ex ^Callable f]
  (.submit ex f))

(defn deref-safely
  "Wait up to `ms` for a future and turn BOTH failure modes into data.

  NOT named `await` on purpose: clojure.core/await exists as of Clojure 1.12
  and shadowing it just produces a warning and confusion.

  `.get` throws, and it throws two different things that are easy to
  conflate. A `TimeoutException` means the task is still running and you gave
  up on it -- note the thread is NOT cancelled, it keeps going. An
  `ExecutionException` means the task itself threw, and the real cause is
  wrapped one level down, so you must unwrap with `ex-cause` to see it.

  Returning a Throwable as a value instead of throwing is what lets the six
  approaches in this namespace all have the same signature."
  [^Future f ^long ms]
  (try
    (.get f ms TimeUnit/MILLISECONDS)
    (catch TimeoutException t     ::timeout)
    (catch ExecutionException e   (ex-cause e))))

(defmacro with-permit
  "Run `body` holding one permit from a fair `Semaphore`.

  WHY A SEMAPHORE AND NOT A THREAD POOL. Virtual threads are cheap, so you
  should NOT pool them -- make one per task and let it die. But cheap threads
  do not mean you may run unbounded concurrency against a dependency; that is
  still your dependency's problem. A semaphore decouples the number of tasks
  you are willing to create from the number of requests you are willing to
  have in flight, which is the limit you actually care about.

  `true` in `(Semaphore. n true)` requests FIFO fairness. Without it a
  semaphore is not fair and a steady stream of new arrivals can starve older
  waiters. Always pass `true` for a resource limiter.

  The release MUST be in a `finally`. If the body throws and you skip the
  release, every subsequent caller blocks forever and the whole program
  deadlocks -- the permit is gone and nothing will ever return it."
  [sem & body]
  `(do (.acquire ^Semaphore ~sem)
       (try ~@body
            (finally (.release ^Semaphore ~sem)))))

(defn outcome
  "Normalise whatever `deref-safely` gave us into the common return shape."
  [section v]
  (cond
    (identical? v ::timeout)      {:section section :status :timeout}
    (identical? v ::scope-closed) {:section section :status :cancelled}
    (instance? Throwable v)       {:section section :error (ex-message v)}
    :else                         {:section section :body v}))

;; ---------------------------------------------------------------------------
;; 1. Sequential
;; ---------------------------------------------------------------------------

(defn sequential-fetch
  "Fetch every section one after another on the calling thread.

  BENEFIT: no concurrency to reason about, no shared state, stack traces that
  are completely honest, and trivially correct. Always the right answer for
  fewer than a handful of requests, and the baseline every other measurement
  is compared against.

  DRAWBACK: total time is the SUM of every request's time, so latency scales
  linearly with the number of sections. This host takes ~280ms of think time
  per request, so six sections land at ~1.7s -- and that is the honest cost of
  the baseline. Note what it is NOT: connection setup. The handshake is a
  one-off ~1s; the other ~1.6s is the server thinking. Which is why overlapping
  those six calls is the biggest single win available here, and why pmap's
  unlabelled 32-way cap stays invisible until N gets large.

  This is also the ONLY approach here that needs no timeout, because the
  calling thread is the one that would block and you control its lifetime."
  [sections & [{:keys [timeout-ms] :or {timeout-ms 3000} :as _opts}]]
  (mapv (fn [section]
          (outcome section
                   (try (http-fetch section 1)
                        (catch Exception e e))))
        sections))

;; ---------------------------------------------------------------------------
;; 2. pmap  -- the idiomatic four-token version, and its unlabelled 32-way cap
;; ---------------------------------------------------------------------------

(defn pmap-fetch
  "clojure.core/pmap over the sections: the shortest correct-looking parallel form.

  BENEFIT: it is the fastest route from 'serial' to 'concurrent' and it
  preserves order. For small fan-outs it is genuinely as fast as anything else,
  because with N under 32 the chunking cap never bites. That is the whole range
  in which pmap is the right answer.

  DRAWBACK, AND IT IS A SURPRISING ONE. `pmap` is literally
  `(map #(future (f %)) coll)`, and it is LAZY. Lazy seqs are realised in
  chunks of `clojure.core/chunk-size` -- 32 -- so pmap only ever has about 32
  requests in flight no matter how big `coll` is. It is a concurrency cap
  wearing no label. Measured on a 12-core box, 10ms blocking task, against a
  deliberately 32-thread bounded pool doing the same submit-then-collect work:

        N tasks     pmap    32-thread pool    unbounded pool    virtual threads
           1,000    348ms              333ms             73ms             39ms
           3,200  1,052ms            1,034ms            135ms             81ms
         100,000  ~33,000ms                     --             --            216ms

  pmap tracks N/32 x 10ms to within a few percent, and tracks a 32-thread pool
  to within a few percent. It is NOT slow because threads are expensive --
  pre-creating 200 idle OS threads does not change its runtime at all. It is
  slow because it caps you at 32-way concurrency. If 32 in flight is enough,
  pmap is fine. If you need 500 requests at a server that can take them, pmap
  will not get you there and will not tell you why.

  The real thread-explosion hazard lives in the third column: submit all N
  tasks before collecting any, on an UNBOUNDED pool, and you get N OS threads.
  `future` dispatches to `Agent.soloExecutor`, which `javap` shows is built
  with `Executors.newCachedThreadPool`, so that idiom -- which is what you
  write the moment you 'fix' pmap's ordering or want a timeout per element --
  spawns one platform thread per element, each holding ~1MB of stack. pmap's
  chunking is the only thing accidentally standing between you and that.

  It also ignores a `:max-in-flight` option, because there is nowhere to put
  one. Measured against the live API with a ceiling of 4 requested, pmap ran 6
  requests at once and the other five approaches all stayed at 4. For a
  dependency you care about, an approach that cannot tell you how many requests
  it will make is a poor fit no matter how fast it is.

  Also note pmap's own docstring says it is \"only useful for computationally
  intensive functions\" -- that advice predates virtual threads and is about
  coordination overhead, not I/O. For I/O fan-out pmap is fine, up to a point.

  pmap has no timeout and no per-element error capture: one failed request
  throws out of the middle of the sequence and you lose the rest.

  THE TRAP IN THE OBVIOUS 'FIX'. The first version of this function was

      (mapv (fn [section] (outcome section @(future (http-fetch section 1)))) sections)

  and it was completely SERIAL. `pmap` works because its futures are built by a
  lazy `map` and deref'd by a SEPARATE step, so nothing blocks between submit
  and collect. Put the deref -- or a timeout `.get` -- inside the same map
  function and you get: submit task 0, block on it, submit task 1, block on it.
  Measured against the live API: 1720ms 'parallel' against 564ms actually
  parallel, and a peak-concurrency meter read 1. So the two lines below are
  load-bearing; they are the whole difference between pmap and a `mapv` that
  merely looks concurrent.

  The price of this shape is that you no longer get to call `clojure.core/pmap`
  -- you cannot give a future a timeout without deref'ing it yourself, and
  deref'ing it inside the map is what breaks it."
  [sections & [{:keys [timeout-ms] :or {timeout-ms 3000} :as _opts}]]
  (let [;; 1. submit every future first
        futures (mapv (fn [section]
                        (future (try (http-fetch section 1)
                                     (catch Exception e e))))
                      sections)]
    ;; 2. only now wait on any of them
    (mapv (fn [section future] (outcome section (deref-safely future timeout-ms)))
          sections futures)))

;; ---------------------------------------------------------------------------
;; 3. Bounded OS thread pool
;; ---------------------------------------------------------------------------

(defn bounded-pool-fetch
  "A fixed pool of platform threads, submitting all sections before collecting
  any.

  BENEFIT: the number of concurrent requests is a number you chose, and it is
  the number the requests actually see. This is the conservative, boring,
  correct choice, and it has no virtual-thread-specific caveats -- it behaves
  identically on any JVM, including Java 8.

  Its measured runtime is indistinguishable from pmap's at equal concurrency
  (see pmap-fetch: both track a 32-thread pool), which is the point -- pmap
  was not slow, it was throttled to 32. Raise the pool size and this is where
  you get real fan-out.

  DRAWBACK: the pool size is now a hard structural commitment. Too small and
  you leave concurrency on the table; too large and you are spending ~1MB of
  native stack per thread for no reason. Pool threads are also kept alive
  between tasks, so those stacks stay resident rather than being reclaimed
  after each request.

  Reach for this when you cannot yet be sure your code is free of the things
  that make virtual threads unhappy (see 4), or when you must run on a JVM
  older than 21."
  [sections & [{:keys [max-in-flight] :or {max-in-flight 8} :as _opts}]]
  (with-open [^ExecutorService pool (Executors/newFixedThreadPool max-in-flight)]
    (let [;; submit ALL first, then collect. No blocking.
          ;; Measured against the live API: 2068ms serial-ish vs 723ms correct.
          futures (mapv (fn [section]
                          (submit-task pool ^Callable (fn [] [section (http-fetch section 1)])))
                        sections)]
      (mapv (fn [[section result]] (outcome section result))
            (mapv deref futures)))))

;; ---------------------------------------------------------------------------
;; 4. Virtual threads  -- the recommended default for I/O fan-out
;; ---------------------------------------------------------------------------

(defn virtual-thread-fetch
  "One virtual thread per request, throttled by a fair semaphore. This is the
  pattern to reach for by default when you are fanning out over I/O.

  BENEFIT: you keep ordinary straight-line blocking code -- no callbacks, no
  inversion of control, no core.async. Your stack traces look like the code you
  wrote. Concurrency is decoupled from thread cost, so the fan-out size stops
  being a performance decision. And the semaphore gives you exactly the limit
  you want (in-flight requests) without giving up cheap threads.

  DRAWBACKS, all of which are about what a virtual thread is:

  * They are NOT PREEMPTIVE. A virtual thread doing a long calculation will not
    be interrupted, so it can monopolise the carrier (the platform thread it
    is mounted on) and starve every other virtual thread. Rule of thumb: if a
    task is mostly CPU, it belongs on a platform thread or in
    clojure.core.reducers, not here.

  * Do not `swap!`/`alter` inside them. Those retry in a HOT LOOP, and since
    nothing preempts a virtual thread, a contended `swap!` can livelock a
    carrier. Use java.util.concurrent collections for shared state, or better,
    return values and let one thread collect -- which is what this function
    does.

  * Do not touch `synchronized` blocks or methods. They PIN the carrier: the
    virtual thread blocks but the platform thread stays occupied, so the whole
    benefit evaporates. Run with -Djdk.tracePinnedThreads=full to find these.
    (Clojure itself used synchronized for lazy seqs and delay; that was fixed
    in 1.12 by CLJ-2800, so on 1.12+ this is not your problem unless you are
    calling library code that still does it.)

  * `Executors/newVirtualThreadPerTaskExecutor` is UNBOUNDED. That is correct
    for thread count and catastrophic for request count. The semaphore is not
    optional decoration; it is the load-bearing part.

  * A timed-out request here is NOT cancelled -- `await` gives up waiting, the
    virtual thread carries on to completion. The semaphore permit is released
    when it finishes, not when you stop waiting."
  [sections & [{:keys [max-in-flight timeout-ms]
                :or {max-in-flight 8 timeout-ms 3000} :as _opts}]]
  (with-open [^ExecutorService pool (Executors/newVirtualThreadPerTaskExecutor)]
    (let [sem (Semaphore. max-in-flight true)
          ;; ^Callable is essential -- see submit-task above.
          futures (mapv (fn [section]
                          (submit-task pool ^Callable
                                       (fn []
                                         (with-permit sem
                                           (http-fetch section 1)))))
                        sections)]
      (mapv (fn [section future] (outcome section (deref-safely future timeout-ms)))
            sections futures))))

;; ---------------------------------------------------------------------------
;; 5. core.async io-thread  -- the idiomatic channel version
;; ---------------------------------------------------------------------------

(defn core-async-fetch
  "The same idea as 4, expressed with core.async. Its :io executor pool is
  backed by virtual threads on Java 21+, with no configuration required.

  BENEFIT: you get virtual threads for free (verified: inside `io-thread`,
  `(.isVirtual (Thread/currentThread))` is true, while inside `thread` it is
  false), plus channels, which are the right tool once you need to express
  something a return value cannot -- pipelines, streaming results out as they
  arrive, work queues, or a request that fans out into a second round of calls.

  DRAWBACKS:

  * `io-thread` is NOT `go`. This matters and the distinction is easy to get
    wrong. core.async 1.9.829-alpha2 reimplemented `go` blocks on virtual
    threads; 1.9.847-alpha3 REVERTED that, and `go` blocks still run on
    platform threads via the analyzer and inversion of control. The revert
    happened because a `go` block can be garbage collected when its channels
    are collected, while a virtual thread must terminate ordinarily and holds
    its resources until it does. Those semantics cannot be reconciled.

    So: `io-thread` ALLOWS arbitrary blocking I/O and blocking channel ops
    (`<!!`, `>!!`); `go` FORBIDS blocking I/O and allows only parking ops
    (`<!`, `>!`). If you are doing HTTP calls, you want `io-thread`.

  * `io-thread` returns a CHANNEL, not a value, so you read it with `<!!`
    rather than `@` (a channel is not IDeref as of 1.10).

  * `<!!` has NO TIMEOUT ARITY any more. In current core.async it is an
    ordinary function with arglists `([port])`, and `(async/<!! ch 3000 ::to)`
    throws ArityException. The replacement is to race the channel against one
    built by `(async/timeout msecs)`, which returns a channel that closes
    after msecs, using `alts!!`.

  * The submit-all-then-collect trap is still here, and the same rule applies:
    the first `mapv` starts every io-thread before the second one reads any."
  [sections & [{:keys [max-in-flight timeout-ms]
                :or {max-in-flight 8 timeout-ms 3000} :as _opts}]]
  (let [sem (Semaphore. max-in-flight true)]
    (->> sections
         ;; mapv is eager: every io-thread is STARTED here, before we read any.
         (mapv (fn [section]
                 (async/io-thread
                  (with-permit sem
                    (try (http-fetch section 1)
                         (catch Exception e e))))))
         ;; only now do we block on the results
         (mapv (fn [section ch]
                 (outcome section
                          ;; race the result against a channel that closes in
                          ;; timeout-ms. `(= winner ch)` tells us the task
                          ;; actually finished; otherwise the timeout won.
                          ;; Note this bounds how long WE WAIT -- the io-thread
                          ;; is not cancelled and keeps its permit until it
                          ;; finishes. The real timeout is the one on the HTTP
                          ;; client; this one is belt-and-braces.
                          (let [[v winner] (async/alts!! [ch (async/timeout timeout-ms)]
                                                         :priority true)]
                            (if (= winner ch) v ::timeout))))
               sections))))

;; ---------------------------------------------------------------------------
;; 6. Structured concurrency  -- fail fast, guaranteed cleanup
;; ---------------------------------------------------------------------------

(defn structured-fetch
  "StructuredTaskScope: the JDK's answer to 'what if I want the children to
  die when the parent does'.

  BENEFIT: this is the only approach here with a real SCOPE. A
  StructuredTaskScope guarantees that when the block exits -- normally, by
  exception, or by early return -- every subtask it forked has finished or
  been cancelled. It also gives you the timeouts and cancellation that
  `future` has no way to express at all. When one request fails you can either
  fail the whole batch or settle for what you have, as a policy decision
  rather than an ad-hoc rescue.

  DRAWBACKS:

  * It is the most Java-flavoured option in this file. Getting at the static
    Joiner factory from Clojure needs the fully-qualified nested class name
    (`StructuredTaskScope$Joiner/awaitAll`), and the subtasks have to be
    collected by hand because the scope does not expose them.

  * Interop is only reasonable from Java 21, so this is not portable to older
    JVMs at all.

  * No backpressure: it fans out all N immediately. You still need a semaphore
    if N is larger than the dependency can absorb. Structured concurrency
    solves lifetime, not load.

  Note this uses Joiner/awaitAll, which waits for every subtask without
  throwing, then reads each subtask's own outcome. Swap it for
  `allSuccessfulOrThrow` to get fail-fast-and-cancel-siblings instead."
  [sections & [{:keys [max-in-flight timeout-ms]
                :or {max-in-flight 8 timeout-ms 3000} :as _opts}]]
  (let [sem (Semaphore. max-in-flight true)]
    (try
      (with-open [scope (StructuredTaskScope/open
                         ;; awaitAll = wait for everything, no fail-fast.
                         ;; Use allSuccessfulOrThrow for fail-fast.
                         java.util.concurrent.StructuredTaskScope$Joiner/awaitAll)]
        (let [;; .fork hands back a Subtask, which is how you get results back.
              subtasks (mapv (fn [section]
                               (.fork scope ^Callable
                                      (fn []
                                        (with-permit sem
                                          [section
                                           (try (http-fetch section 1)
                                                (catch Exception e e))]))))
                             sections)]
          (.join scope)
          (mapv (fn [section ^StructuredTaskScope$Subtask st]
                  (outcome section
                           ;; Subtask/state() returns the enum
                           ;; UNAVAILABLE | SUCCESS | FAILED, so it has to be
                           ;; turned into something `case` can match on.
                           (case (keyword (.name (.state st)))
                             :SUCCESS     (second (.get st))
                             :FAILED      (.exception st)
                             ;; UNAVAILABLE means the scope shut the task down
                             ;; before it could report -- a real outcome that
                             ;; no clause above covers.
                             :UNAVAILABLE ::scope-closed)))
                sections subtasks)))
      (catch Exception e
        ;; e.g. InterruptedException, or a fail-fast joiner throwing.
        (mapv (fn [section] {:section section :error (ex-message e)}) sections)))))

;; ---------------------------------------------------------------------------
;; The entry point: run all six against the live API, side by side
;; ---------------------------------------------------------------------------

(def ^:private approaches
  [["sequential      " sequential-fetch]
   ["pmap            " pmap-fetch]
   ["bounded pool    " bounded-pool-fetch]
   ["virtual threads " virtual-thread-fetch]
   ["core.async      " core-async-fetch]
   ["structured      " structured-fetch]])

(defn -main
  "Run every approach over the same sections and print them side by side.

  Deliberately frugal: the default is 2 sections, so 12 requests total against
  a free public demo API, with a pause between approaches. Pass a section count
  to go bigger -- and read demo.bench-net's warning first, because at 6 sections
  the numbers stop being comparable to each other.

  The timings here are NOT a ranking. The host is shared and its variance
  exceeds the gaps between these approaches. See demo.bench for the
  deterministic version and demo.bench-net for where the time actually goes."
  [& args]
  (let [n        (if (first args) (Long/parseLong (first args)) 2)
        sections (vec (take n (keys sections)))
        opts     {:max-in-flight 4 :timeout-ms 5000}]
    (println (str "\nSIX WAYS TO FETCH THE SAME THINGS  --  " base-url))
    (println (apply str (repeat 62 "=")))
    (println (format "sections: %s   |   %d requests, 4 in flight max"
                     (pr-str sections) (* n (count approaches))))
    (println "times are NOT a ranking: the host is shared and noisy.\n")

    ;; Warm the connection pool before timing anything, and warm it
    ;; CONCURRENTLY. All six approaches share one defonce connection manager, so
    ;; whichever ran first would otherwise be the only one paying the ~1s
    ;; DNS+TCP+TLS handshake while the rest reused its warm connections -- a
    ;; ~1s artefact of ordering, not a property of that approach. (A
    ;; long-running process is always warm, so this is also the realistic case.)
    ;;
    ;; The concurrency matters too, and it is its own small lesson: warming
    ;; sequentially leaves ONE connection in the pool, so the first approach
    ;; that asks for two requests at once makes the pool lease a second
    ;; connection while the first is in flight. That showed up as pmap -- which
    ;; runs second, and is the first to go parallel -- coming in at 860ms on
    ;; every run while pmap timed again later in the same process came in at
    ;; 334ms, identical to the virtual-thread version. Pool lease contention,
    ;; not concurrency cost.
    (let [warm (mapv (fn [section]
                       (future (try (http-fetch section 1)
                                    (catch Exception _ nil))))
                     sections)]
      (doseq [f warm] (deref f 5000 ::timed-out)))
    (Thread/sleep 1000)
    (println (format "   (warmed %d pooled connection(s), untimed)\n" (count sections)))

    (println (format "   %-16s %10s %6s  %s" "approach" "time" "ok" "first result"))
    (doseq [[label fetch] approaches]
      (Thread/sleep 700)
      (let [t      (System/nanoTime)
            result (try (fetch sections opts)
                        (catch Throwable e [[:crashed (ex-message e)]]))
            ms     (/ (- (System/nanoTime) t) 1e6)
            ok     (count (filter :body result))
            title  (some-> result first :body :title)
            preview (pr-str (when title (subs title 0 (min 30 (count title)))))]
        (println (format "   %-16s %7.0f ms %4d/%d  %s"
                         (string/trim label) ms ok n preview))))
    (println "\nSame signature, same return shape for all six:")
    (println "  (fetch-all sections)      ->  (fetch-all sections opts)")
    (println "  [{:section :posts :body {...}}  {:section :users :body {...}} ...]")
    (println "  failures come back as data (:error / :status :timeout), never thrown,")
    (println "  so a benchmark here measures concurrency and nothing else.")
    (println "\nFor real numbers see demo.bench (deterministic, no network) and")
    (println "demo.bench-net (live, budgeted, and honest about its own noise).")
    (shutdown-agents)))
