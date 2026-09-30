(ns main
  (:import (java.util.concurrent Executors)))

;; from https://www.braveclojure.com/concurrency/
;; 1. Write a function that takes a string as an argument and searches for it on Bing and Google using 
;;    the slurp function. Your function should return the HTML of the first page returned by the search.
;; 2. Update your function so it takes a second argument consisting of the search engines to use.
;; 3. Create a new function that takes a search term and search engines as arguments, and returns a
;;    vector of the URLs from the first page of search results from each search engine.

(def sections {:posts "posts"
               :comments "comments"
               :albums "albums"
               :photos "photos"
               :todos "todos"
               :users "users"})

(defn get-mock
  [section i]
  (slurp
   (format "https://jsonplaceholder.typicode.com/%s/%s" section i)))

;; Fetch requests sequentially
(defn get-mock-sections
  "Get item i from all specified sections"
  [sections i]
  (map #(get-mock % i) sections))

(println "3 sections sequential")
#_(time
   (doall
    (get-mock-sections
     (mapv #(% sections) [:posts :comments :users])
     1)))

(println "6 sections sequential")
#_(time
   (doall
    (get-mock-sections
     (mapv #(% sections) [:posts :comments :users :albums :photos :todos])
     1)))

;; ## Now let's make it concurent

;; ### Test with parallel fetching using OS threads

;; Fetch requests in parallel with pmap
(defn get-mock-sections-p
  "Get item i from all specified sections"
  [sections i]
  (pmap #(get-mock % i) sections))

(println "3 sections pmap")
#_(time
   (doall
    (get-mock-sections-p
     (mapv #(% sections) [:posts :comments :users])
     1)))

(println "6 sections pmap")
#_(time
   (doall
    (get-mock-sections-p (mapv #(% sections) [:posts :comments :users :albums :photos :todos]) 1)))

;; ### Using Virtal Threads
(defonce executor (Executors/newVirtualThreadPerTaskExecutor))
(let [posts (.submit executor #(get-mock (:posts sections) 1))]
  @posts)

(defn get-mock-sections-vt
  [sections i]
  (map
   (fn [section] (.submit executor #(get-mock section i)))
   sections))
