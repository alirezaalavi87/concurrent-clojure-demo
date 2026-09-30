(ns demo.bench-net
  "A live benchmark against jsonplaceholder.typicode.com.

  THIS BENCHMARK IS POLITE BY CONSTRUCTION. jsonplaceholder is a free public
  demo API with no rate-limit headers and no SLA, and a benchmark that hammers
  it is a small denial-of-service tool. Three independent brakes are applied:

    1. A HARD REQUEST BUDGET. `reserve!` claims requests from an atomic
       counter BEFORE the work starts, and throws rather than going over. The
       count is printed on exit, including from a shutdown hook, so an aborted
       run still reports what it spent.
    2. A CONCURRENCY CEILING passed into each approach as `:max-in-flight`,
       which each approach enforces itself -- a fair semaphore for the virtual
       and core.async versions, a fixed-size pool for the bounded one. Section 3
       MEASURES the resulting peak rather than trusting the claim.
    3. AN INTER-ROUND DELAY, so consecutive approaches do not arrive as a
       burst.

  WHAT THIS BENCHMARK CANNOT TELL YOU: which approach is faster. The variance
  is larger than the effect. The same pmap code has been observed running in
  771ms on one pass and 1368ms on the next against this host, and the
  virtual-thread version degraded from 2029ms to 4124ms across rounds. Use it
  for the SHAPE of the timings -- especially the connect-vs-body split, which
  is stable and is the actionable finding -- and use demo.bench for any claim
  about relative speed.

  Run it:  clojure -M -m demo.bench-net
           clojure -M -m demo.bench-net 3        (fewer sections)
           clojure -M -m demo.bench-net 6 2      (6 sections, 2 in flight)"
  (:require [clojure.string :as str]
            [demo.core :as core])
  (:import (java.net URL)))

;; ---------------------------------------------------------------------------
;; Brake 1: the request budget
;; ---------------------------------------------------------------------------

(def ^:private request-cap (atom 60))
(def ^:private requests-sent (atom 0))

(defn- reserve!
  "Atomically claim `n` requests from the budget, or throw.

  Claiming BEFORE the work rather than counting after is what makes this a real
  cap: a round that would overshoot is refused, not sent and regretted.
  `swap!` is atomic, so concurrent tasks cannot race past the limit.

  Note what this does and does not cover. It counts REQUESTS THE BENCHMARK
  DECIDES TO MAKE, one per fetch, on the assumption that each fetch is exactly
  one HTTP GET. It is not a socket-level guard: a client-level retry or
  redirect would slip past it. That is why the concurrency ceiling exists as an
  independent second brake."
  [n]
  (let [after (swap! requests-sent + n)]
    (when (> after @request-cap)
      (swap! requests-sent - n)
      (throw (ex-info "request budget exhausted -- not sending"
                      {:budget @request-cap :already-spent (dec after)})))))

(defn budget-report []
  (format "%d/%d requests sent" @requests-sent @request-cap))

;; Report on exit however we leave, including Ctrl-C -- which is exactly when
;; you most want to know what the run cost.
(.addShutdownHook
 (Runtime/getRuntime)
 (Thread. ^Runnable (fn [] (println (str "\n[budget] " (budget-report))))))

;; ---------------------------------------------------------------------------
;; Brake 2, verified: measure each approach's real peak concurrency
;; ---------------------------------------------------------------------------

(defn- measure-in-flight
  "Run `f`, instrumenting `core/http-fetch` to record true peak concurrency.

  All six approaches funnel every request through `core/http-fetch`, and Clojure
  resolves that call through the var, so `with-redefs` sees them all. This turns
  'the semaphore bounds concurrency' from a comment into a number -- and it is
  worth checking, because a semaphore in the wrong place bounds nothing."
  [f]
  (let [orig  core/http-fetch
        count (atom 0)
        peak  (atom 0)
        instrumented
        (fn [section i]
          (swap! count inc)
          (swap! peak max @count)
          (try
            (orig section i)
            (finally (swap! count dec))))
        result (with-redefs [core/http-fetch instrumented] (f))]
    [result @peak]))

;; ---------------------------------------------------------------------------
;; Finding 1: where does a request's time actually go?
;; ---------------------------------------------------------------------------

(defn split-timings
  "Make ONE request and time it in three phases.

  `.connect` returns once DNS, TCP and TLS are done and the request is on the
  wire; `.getResponseCode` returns once the response HEADERS are back; the
  slurp then pulls the body. Splitting it this way is the entire point.

  Uses HttpURLConnection rather than clj-http because a client that pools and
  retries hides the handshake -- and the handshake is the expensive part. This
  is the same underlying stack clj-http uses."
  [url]
  (let [conn ^java.net.HttpURLConnection (.openConnection (URL. url))]
    (.setRequestMethod conn "GET")
    (.setRequestProperty conn "User-Agent" "concurrent-clj-demo")
    (.setConnectTimeout conn 5000)
    (.setReadTimeout conn 5000)
    (let [t0     (System/nanoTime)
          _      (.connect conn)
          t1     (System/nanoTime)
          status (.getResponseCode conn)
          t2     (System/nanoTime)
          body   (if (<= 200 status 299) (slurp (.getInputStream conn)) "")
          t3     (System/nanoTime)]
      {:status      status
       :bytes       (count body)
       :connect-ms  (/ (- t1 t0) 1e6)
       :headers-ms  (/ (- t2 t1) 1e6)
       :body-ms     (/ (- t3 t2) 1e6)
       :total-ms    (/ (- t3 t0) 1e6)})))

(defn benchmark-where-the-time-goes
  "A few sequential requests, split into connect / headers / body.

  Connection setup dominates against this host. That is why a baseline of ~2s
  for six small JSON reads is not a mystery, and it is the number to fix first
  before reaching for threads."
  [n]
  (println (format "\n1. Where does the time go?  (%d sequential requests, no concurrency)" n))
  (println (format "   %-14s %10s %9s %9s %8s" "url" "connect" "headers" "body" "bytes"))
  (let [rows (doall
               (for [i (range n)]
                 (do (reserve! 1)
                     (let [r (split-timings (str core/base-url "/users/" (+ 1 i)))]
                       (println (format "   %-14s %8.0f ms %7.0f ms %7.0f ms %8d"
                                        (str "/users/" (+ 1 i))
                                        (:connect-ms r) (:headers-ms r) (:body-ms r) (:bytes r)))
                       (Thread/sleep 400)
                       r))))]
    ;; Report what was measured rather than a remembered number: the handshake
    ;; varies a lot between runs and the footer must not contradict the table.
    (let [first-connect (:connect-ms (first rows))
          rest-connect  (map :connect-ms (rest rows))
          headers       (map :headers-ms rows)]
      (println (format "   -> The handshake is a ONE-OFF: %.0fms on the first call, %s after."
                       first-connect
                       (str/join ", " (map #(format "%.0fms" %) rest-connect))))
      (println (format "      The real per-request cost is the ~%.0fms wait for response"
                       (double (/ (reduce + headers) (count headers)))))
      (println "      headers, i.e. the SERVER thinking; the body is a couple of ms.")
      (println "      So: pool the connection, then overlap the wait. In that order."))))

;; ---------------------------------------------------------------------------
;; Finding 2: connection reuse
;; ---------------------------------------------------------------------------

(defn benchmark-connection-reuse
  "Compare `slurp` with clj-http (Apache HttpClient, which pools).

  A common claim is that `slurp` opens a new connection per call and so pays a
  handshake every time. On HotSpot that is false: `HttpURLConnection` has a
  built-in keep-alive cache, so repeated calls to one host reuse the connection.
  Two rounds of the same three requests show it -- round 2 is no slower for
  either client.

  What is TRUE is that the handshake is a one-off per connection, so the first
  request dominates a short run, and a real pooled client gives you control
  (size, timeouts, per-route ceilings) instead of the JVM's 5-entry default."
  []
  (println "\n2. Connection reuse -- no concurrency involved at all")
  (doseq [round [1 2]]
    (let [slurp-ms (let [t (System/nanoTime)]
                     (dotimes [i 3]
                       (reserve! 1)
                       (slurp (str core/base-url "/users/" (+ 1 i))))
                     (/ (- (System/nanoTime) t) 1e6))
          http-ms  (let [t (System/nanoTime)]
                     (dotimes [i 3]
                       (reserve! 1)
                       (core/http-fetch :users (+ 1 i)))
                     (/ (- (System/nanoTime) t) 1e6))]
      (println (format "   round %d   slurp (JVM keep-alive cache) %7.0f ms   |   clj-http (pooled) %7.0f ms"
                       round slurp-ms http-ms)))
    (Thread/sleep 500))
  (println "   -> slurp is NOT paying a handshake per call: round 2 is no slower.")
  (println "      The whole gap is the first request's handshake, which both")
  (println "      clients pay once. Pool anyway -- for the timeouts and the control,")
  (println "      not because slurp reconnects."))

;; ---------------------------------------------------------------------------
;; The main event: the six approaches, same sections, same budget
;; ---------------------------------------------------------------------------

(def ^:private approaches
  [["sequential      " core/sequential-fetch]
   ["pmap            " core/pmap-fetch]
   ["bounded pool    " core/bounded-pool-fetch]
   ["virtual threads " core/virtual-thread-fetch]
   ["core.async      " core/core-async-fetch]
   ["structured      " core/structured-fetch]])

(defn benchmark-approaches
  "Run every approach over the same sections, with a pause between rounds.

  `opts` carries `:max-in-flight`, which is brake 2 -- each approach enforces
  it in its own way, and the measured peak column checks that it worked."
  [section-names opts concurrency round-delay-ms]
  (let [n (count section-names)]
    (println (format "\n3. The six approaches over %d sections (ceiling: %d in flight)" n concurrency))
    ;; Section 2 above has already warmed the shared connection pool, so no
    ;; approach is charged for the ~1s DNS+TCP+TLS handshake here. That matters:
    ;; all six share one defonce connection manager, so whichever ran first
    ;; would otherwise be the only one paying it. In a long-running process the
    ;; pool is warm anyway, so this is also the realistic case.
    (println (format "   %-16s %10s %7s %10s  %s" "approach" "time" "ok" "peak conc" "note"))
    (doseq [[label fetch] approaches]
      (Thread/sleep round-delay-ms)
      (let [t (System/nanoTime)
            outcome
            (try
              (reserve! n)
              (let [[result peak] (measure-in-flight #(fetch section-names opts))
                    ms (/ (- (System/nanoTime) t) 1e6)
                    errs (remove :body result)]
                {:ms ms
                 :ok (count (filter :body result))
                 :peak peak
                 :note (cond
                         ;; The most interesting row in the table. An approach
                         ;; that cannot honour a concurrency ceiling is a poor
                         ;; fit for someone else's server, however fast it is.
                         (> peak concurrency)
                         (format "!! IGNORED the ceiling of %d -- no way to bound it"
                                 concurrency)

                         (seq errs)
                         (format "%d section(s) not 200: %s"
                                 (count errs)
                                 (str/join ", " (map #(str (:section %) "=" (:status % (:error %))) errs)))

                         :else "")})
              (catch Exception e
                {:ms (- (/ (- (System/nanoTime) t) 1e6))
                 :ok 0 :peak 0 :note (ex-message e)}))]
        (println (format "   %-16s %7.0f ms %4d/%d %9d  %s"
                         (str/trim label) (:ms outcome) (:ok outcome) n (:peak outcome)
                         (:note outcome)))))))

;; ---------------------------------------------------------------------------

(defn -main [& args]
  (let [[n-arg conc-arg] args
        n        (if n-arg (Long/parseLong n-arg) 6)
        sections (vec (take n (keys core/sections)))
        conc     (if conc-arg (Long/parseLong conc-arg) 4)
        opts     {:max-in-flight conc :timeout-ms 5000}]
    (println (str "\nLIVE BENCHMARK vs " core/base-url))
    (println (apply str (repeat 62 "=")))
    (println (format "budget: %d requests max, %d concurrent ceiling, %dms between rounds"
                     @request-cap conc 2000))
    (println (format "cores: %d   jvm: %s"
                     (.availableProcessors (Runtime/getRuntime))
                     (System/getProperty "java.version")))

    (benchmark-where-the-time-goes 3)
    (benchmark-connection-reuse)
    (benchmark-approaches sections opts conc 2000)

    (println (str "\n[budget] " (budget-report)))
    (println "Read finding 1 (connect vs body) and finding 2 (connection reuse) --")
    (println "those are actionable and stable. Do NOT read section 3 as a ranking:")
    (println "this host is shared and the run-to-run variance exceeds the gaps.")
    (shutdown-agents)))
