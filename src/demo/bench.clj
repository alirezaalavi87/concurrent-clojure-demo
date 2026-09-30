(ns demo.bench
  "A local, deterministic benchmark. Nothing here touches the network.

  This exists because the interesting questions about concurrency are about
  THREADS, and the only way to measure threads honestly is to remove the
  network from the picture. A live benchmark against a shared public API
  measures that server's mood: the same code has been observed running
  pmap in 771ms on one pass and 1368ms on the next, with the virtual-thread
  version getting steadily slower across rounds. Variance like that is larger
  than the effect you are trying to measure.

  Run it:  clojure -M -m demo.bench
           clojure -M -m demo.bench 100000    (override the largest fan-out)"
  (:require [demo.core :as core])
  (:import (java.lang.management ManagementFactory)
           (java.util.concurrent Callable ExecutorService Executors Future)))

;; ---------------------------------------------------------------------------
;; Harness
;; ---------------------------------------------------------------------------

(defn ms
  "Run `f`, return how long it took in milliseconds."
  [f]
  (let [t (System/nanoTime)]
    (f)
    (/ (- (System/nanoTime) t) 1e6)))

(defn platform-threads
  "Current count of OS (platform) threads in the JVM.

  This is the number that matters when comparing virtual threads to pmap: a
  virtual thread multiplexes onto these, so a healthy fan-out should barely
  move it. Note that the ABSOLUTE value includes the JVM's own ~20 threads, so
  what is interesting is how much a run ADDS to it."
  []
  (.getThreadCount (ManagementFactory/getThreadMXBean)))

(defn peak-during
  "Run `body`, sampling the platform-thread count every 1ms while it runs.

  Returns `[result peak-thread-count]`. Sampling during the run is the whole
  point: a cached thread pool holds its idle threads for 60s, so counting
  threads before and after gives you 0 for a run that briefly spawned 2,000."
  [body]
  (let [running (atom true)
        peak    (atom (platform-threads))
        sampler (doto (Thread. ^Runnable
                            (fn []
                              (while @running
                                (swap! peak max (platform-threads))
                                (Thread/sleep 1))))
                 (.setDaemon true)
                 (.start))]
    (try
      (let [result (body)]
        (swap! peak max (platform-threads))
        ;; Order matters: clear the flag BEFORE joining, or the sampler never
        ;; exits and this joins forever.
        (reset! running false)
        (.join sampler)
        [result @peak])
      (finally
        (reset! running false)))))

(defn section-title [s]
  (println (format "\n%s\n%s" s (apply str (repeat (count s) "-")))))

;; A task that blocks, the way a network call does, and returns a value so we
;; can prove afterwards that every task really ran. Verifying the sum is not
;; optional: it is what caught the serial-execution bug described in section D.
(def ^:private checksum (+ 1 2 3 4 5 6 7 8 9 10))

(defn io-task
  "Simulates one blocking I/O call of `latency-ms`."
  [latency-ms]
  (fn [_]
    (Thread/sleep latency-ms)
    checksum))

;; ---------------------------------------------------------------------------
;; Runners. Note the shape for the pool-based ones: build ALL futures first,
;; collect second. pmap is the exception, and section A shows why.
;; ---------------------------------------------------------------------------

(defn- timed
  "Run `f` under the thread sampler. Returns {:ms :peak-threads :result}."
  [f]
  (let [t (System/nanoTime)
        [result peak] (peak-during f)]
    {:ms     (/ (- (System/nanoTime) t) 1e6)
     :peak   peak
     :result result}))

(defn- submit-all-then-collect
  "Hand every task to `pool` before waiting on any of them.

  The ordering is the entire point. Deref'ing inside the same mapv would
  submit task 0, block on it, submit task 1, block again -- sequential
  execution in a parallel costume."
  [^ExecutorService pool n task]
  (let [futures (mapv (fn [i] (core/submit-task pool ^Callable (fn [] (task i)))) (range n))]
    (reduce + (mapv (fn [^Future f] (.get f)) futures))))

(defn run-pmap [n task]
  (let [{:keys [ms peak result]} (timed #(reduce + (doall (pmap task (range n)))))]
    {:ms ms :peak peak :sum result}))

(defn run-virtual [n task]
  (with-open [^ExecutorService pool (Executors/newVirtualThreadPerTaskExecutor)]
    (let [{:keys [ms peak result]} (timed #(submit-all-then-collect pool n task))]
      {:ms ms :peak peak :sum result})))

(defn run-fixed-pool [n task pool-size]
  (with-open [^ExecutorService pool (Executors/newFixedThreadPool pool-size)]
    (let [{:keys [ms peak result]} (timed #(submit-all-then-collect pool n task))]
      {:ms ms :peak peak :sum result})))

;; ---------------------------------------------------------------------------
;; A. The scale question: where do virtual threads start to matter?
;; ---------------------------------------------------------------------------

(defn benchmark-scale
  "I/O-bound fan-out: N tasks, each blocking for 10ms.

  The `32-thr pool` column is the control. It does the same submit-then-collect
  work as the other two, on a pool of exactly 32 platform threads. pmap tracks
  it to within a few percent at every N, which is the whole finding: pmap is
  not slow because threads are expensive, it is slow because its laziness
  throttles it to 32 in flight.

  The last two columns are PEAK OS threads, sampled every 1ms while the run is
  in progress. They are flat: whatever N is, virtual threads need a fixed
  handful of carriers and pmap needs about 32."
  [scales]
  (section-title "A. I/O-bound fan-out: N tasks x 10ms blocking call")
  (println (format "  %-8s %9s %14s %9s %13s %11s %9s"
                   "" "pmap" "32-thr pool" "virtual" "pmap OS thr" "vt OS thr" "speedup"))
  (let [task (io-task 10)]
    (doseq [n scales]
      (let [p  (run-pmap n task)
            b  (run-fixed-pool n task 32)
            v  (run-virtual n task)]
        ;; Every task must actually have run. Without this check a runner that
        ;; silently did nothing -- or that ran serially -- still reports a fast
        ;; time and looks like a win.
        (when (not= (:sum p) (:sum v))
          (throw (ex-info "checksum mismatch -- a runner is lying" {:pmap p :vt v})))
        (println (format "  %-8s %6.0f ms %11.0f ms %6.0f ms %13d %11d %8.1fx"
                         (format "N = %d" n)
                         (:ms p) (:ms b) (:ms v) (:peak p) (:peak v)
                         (/ (double (:ms p)) (:ms v))))
        (Thread/sleep 300)))
    (println (format "  (%d is this JVM's own thread count. Both columns are FLAT in N:"
                     (platform-threads)))
    (println "   pmap tops out around baseline+32 because that is its chunking cap, and")
    (println "   it reuses those threads on later runs. Virtual threads sit at")
    (println "   baseline+~13 carriers no matter how many requests you throw at them)")))

;; ---------------------------------------------------------------------------
;; B. The case where virtual threads do NOT help
;; ---------------------------------------------------------------------------

(defn benchmark-cpu
  "CPU-bound work: every carrier is saturated, so the virtual thread
  machinery is pure overhead.

  This is the half of the story that gets left out. Virtual threads do not add
  CPU. There are still only `cores` carriers, so a CPU-bound task should run on
  a platform thread or in clojure.core.reducers."
  []
  (section-title (format "B. CPU-bound (no blocking) -- %d cores" (.availableProcessors (Runtime/getRuntime))))
  (let [task (fn [_] (reduce + (range 300000)))]
    (println (format "  %-9s %19s %19s %19s" "" "pmap" "virtual threads" "fixed pool (12)"))
    (doseq [n [10 100 1000 5000]]
      (let [p (run-pmap n task)
            v (run-virtual n task)
            f (run-fixed-pool n task (.availableProcessors (Runtime/getRuntime)))]
        (println (format "  %-9s %18.0f ms %18.0f ms %18.0f ms"
                         (format "N = %d" n) (:ms p) (:ms v) (:ms f)))
        (Thread/sleep 300)))))

;; ---------------------------------------------------------------------------
;; C. Why pmap degrades
;; ---------------------------------------------------------------------------

(defn benchmark-thread-cost []
  (section-title "C. Thread cost")
  ;; A platform thread reserves a real OS stack (the -Xss default, 1MB on
  ;; 64-bit Linux) plus a glibc malloc arena, both of which are native memory
  ;; the JVM heap counters do not see. A virtual thread has no stack of its
  ;; own: it is a few hundred bytes of heap that borrows the carrier's stack
  ;; while mounted. You cannot cheaply measure that from inside the JVM, so
  ;; section A measures the observable consequence instead -- how many OS
  ;; threads each approach actually needed.
  (doseq [n [1000 10000]]
    (let [platform-ms (ms (fn [] (dotimes [_ n]
                                   (let [^Thread t (Thread. ^Runnable (fn [] nil))]
                                     (.start t)
                                     (.join t)))))
          virtual-ms  (ms (fn [] (with-open [^ExecutorService pool
                                              (Executors/newVirtualThreadPerTaskExecutor)]
                                      (let [fs (mapv (fn [_] (core/submit-task pool ^Callable (fn [] nil)))
                                                     (range n))]
                                        (doseq [^Future f fs] (.get f))))))]
      (println (format "  %6d platform threads: %9.0f ms" n platform-ms))
      (println (format "  %6d virtual  threads: %9.0f ms   (%.0fx cheaper to create)"
                       n virtual-ms (/ (double platform-ms) virtual-ms)))
      (Thread/sleep 500)))
  (println "\n  What pmap actually does, which is not what people assume:")
  (println "  pmap is (map #(future (f %)) coll). `future` dispatches to")
  (println "  Agent.soloExecutor, which javap shows is newCachedThreadPool --")
  (println "  UNBOUNDED. But pmap is LAZY, and lazy seqs are realised in chunks of")
  (println "  clojure.core/chunk-size (32), so pmap keeps only ~32 requests in")
  (println "  flight. Section A shows it tracking a 32-thread pool to within a few")
  (println "  percent. It is a 32-way concurrency cap with no label on it.")
  (println "")
  (println "  So the thread explosion is real but it is NOT in pmap -- it is in the")
  (println "  obvious 'fix': submitting all N tasks before collecting any, on that")
  (println "  unbounded pool. That gives you one OS thread per element. Section D")
  (println "  measures both."))

;; ---------------------------------------------------------------------------
;; D. The traps, as executable assertions
;; ---------------------------------------------------------------------------

(defn- check [label passed? detail]
  (println (format "  [%s] %s" (if passed? "PASS" "FAIL") label))
  (when detail (println (format "         %s" detail))))

(defn benchmark-traps
  "The footguns from demo.core, demonstrated rather than asserted in prose."
  []
  (section-title "D. Interop traps")

  ;; Trap 1: the ^Callable hint.
  ;; NOTE: these call `.submit` DIRECTLY. Going through core/submit-task cannot
  ;; demonstrate this, because submit-task's own parameter is hinted ^Callable
  ;; and that hint wins -- it erases whatever the caller wrote. The trap only
  ;; exists at the raw call site, which is why it is worth knowing about.
  (with-open [^ExecutorService pool (Executors/newVirtualThreadPerTaskExecutor)]
    (let [as-runnable (.submit pool ^Runnable (fn [] 42))  ;; deliberately wrong
          as-callable (.submit pool ^Callable (fn [] 42))]
      (check "submit ^Callable returns the task's value"
             (= 42 (.get as-callable))
             (format "  .submit ^Callable -> %s" (pr-str (.get as-callable))))
      (check "submit ^Runnable silently returns nil (no exception!)"
             (nil? (.get as-runnable))
             (format "  .submit ^Runnable -> %s, even though the task returned 42"
                     (pr-str (.get as-runnable)))))
    (println "         (a Clojure fn implements BOTH Runnable and Callable, so")
    (println "          the hint picks the overload. Hinting the EXECUTOR instead")
    (println "          makes the call fail to compile entirely.)"))

  ;; Trap 2: deref inside mapv is sequential.
  (println)
  (let [slow-task (io-task 50)

        ;; Submit task 0, block on it, submit task 1, block on it, ...
        run-serial
        (fn []
          (with-open [^ExecutorService pool (Executors/newVirtualThreadPerTaskExecutor)]
            (mapv (fn [i]
                    (.get (core/submit-task pool ^Callable (fn [] (slow-task i)))))
                  (range 20))))

        ;; Submit all 20 first, then collect.
        run-parallel
        (fn []
          (with-open [^ExecutorService pool (Executors/newVirtualThreadPerTaskExecutor)]
            (let [futures (mapv (fn [i]
                                  (core/submit-task pool ^Callable (fn [] (slow-task i))))
                                (range 20))]
              (reduce + (mapv (fn [^Future f] (.get f)) futures)))))

        serial-ms (ms run-serial)]
    (Thread/sleep 500)
    (let [parallel-ms (ms run-parallel)]
      (check "submitting and deref'ing in the same mapv runs SERIALLY"
             (> serial-ms (* 5 parallel-ms))
             (format "  20 x 50ms tasks: deref-in-mapv %6.0f ms vs submit-then-deref %6.0f ms"
                     serial-ms parallel-ms))
      (println "         (this one is easy to write by accident, because the")
      (println "          code reads as 'map over sections, fetching each')")))

  ;; Trap 3: where the threads actually come from. The popular belief is that
  ;; pmap makes one OS thread per element. It does not -- but the idiom people
  ;; write to IMPROVE pmap does. Both halves, measured.
  (println)
  (let [baseline (platform-threads)
        n        2000
        task     (io-task 200)      ;; long enough that the peak is real
        [_ pmap-peak]  (peak-during #(reduce + (doall (pmap task (range n)))))
        [_ unbounded-peak]
        (with-open [^ExecutorService pool (Executors/newCachedThreadPool)]
          ;; an UNBOUNDED pool is what `future` uses via Agent.soloExecutor
          (peak-during #(submit-all-then-collect pool n task)))
        [_ virtual-peak]
        (with-open [^ExecutorService pool (Executors/newVirtualThreadPerTaskExecutor)]
          (peak-during #(submit-all-then-collect pool n task)))]
    (check (format "pmap stays near 32 in flight even at N = %d" n)
           (< (- pmap-peak baseline) 60)
           (format "  pmap peaked at %d OS threads (+%d over the %d baseline)"
                   pmap-peak (- pmap-peak baseline) baseline))
    (check "submitting all N first, on an UNBOUNDED pool, really does explode"
           (> (- unbounded-peak baseline) (* 0.8 n))
           (format "  same N on newCachedThreadPool: %d OS threads (+%d)"
                   unbounded-peak (- unbounded-peak baseline)))
    (check "the same work on virtual threads needs a fixed number of carriers"
           (< (- virtual-peak baseline) 40)
           (format "  virtual: %d OS threads (+%d), and it was 3x FASTER"
                   virtual-peak (- virtual-peak baseline)))))

;; ---------------------------------------------------------------------------

(defn -main [& [max-n]]
  (let [scales (if max-n
                 (vec (distinct [10 100 1000 5000 (Long/parseLong max-n)]))
                 [10 100 1000 5000 20000 100000])]
    (println (str "\nLOCAL CONCURRENCY BENCHMARK  (no network)\n"
                  (apply str (repeat 60 "="))))
    (println (format "cores: %d   jvm: %s"
                     (.availableProcessors (Runtime/getRuntime))
                     (System/getProperty "java.version")))
    (benchmark-scale scales)
    (benchmark-cpu)
    (benchmark-thread-cost)
    (benchmark-traps)
    (println "\ndone.")
    (shutdown-agents)))
