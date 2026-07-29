(ns run
  "Conformance against a mock that is no stronger than Kubo, plus an
  executable demonstration of the limit that makes this backend
  single-writer.

  The previous version of this file tested against a mock whose
  `publish-name!` performed a real compare-and-swap on `expected-sequence`
  and whose `resolve-name` returned the stored sequence. Kubo does neither:
  `name/publish` replaces the record unconditionally and `name/resolve`
  returns a path with no sequence at all. So the suite was passing against
  a transport stronger than the one this adapter actually runs on, which is
  the one arrangement in which a green suite tells you nothing.

  The mock below is deliberately weak in exactly the ways Kubo is weak."
  (:require [kotobase.storage.async-contract :as contract]
            [kotobase.storage.core :as storage]
            [kotobase.storage.ipfs :as ipfs]))

(def ^:private failures
  "Counted explicitly, with the process exiting on this rather than on
  `process.exitCode`, which the runner may reset before the promise chain
  settles. Verified by breaking a check on purpose: with the implicit form
  a failing run still exited 0, so CI would have been green forever."
  (atom 0))

(defn- expect [ok? message]
  (if ok?
    (println (str "ok  - " message))
    (do (js/console.error (str "FAIL: " message)) (swap! failures inc))))

(defn- ipfs-node
  "One shared IPFS node. `names` is shared state so that two adapter
   instances -- standing in for two processes -- publish to the same key."
  []
  (let [blocks (atom {})
        names (atom {})]
    {:node/names names
     :put-block! (fn [cid bytes]
                   (swap! blocks assoc cid bytes)
                   (js/Promise.resolve cid))
     :get-block (fn [cid] (js/Promise.resolve (get @blocks cid)))
     ;; A path, and nothing else. No sequence, because Kubo does not
     ;; return one here.
     :resolve-name (fn [_key name]
                     (js/Promise.resolve
                      (when-let [cid (get @names name)]
                        {:cid cid :sequence nil})))
     ;; Unconditional. `expected-sequence` arrives and is ignored,
     ;; because IPNS has nothing to condition on.
     :publish-name! (fn [{:keys [name cid]}]
                      (swap! names assoc name cid)
                      (js/Promise.resolve {:cid cid :sequence nil}))}))

(defn- adapter [node] (ipfs/open {:client node :ipns-key "test-writer"}))

;; ── the limit, made reproducible ─────────────────────────────────────────────

(defn- demonstrate-lost-update
  "Two adapter instances over one node, both publishing from the same head.

   This is what a second process does. Within one instance `serialize!`
   funnels publications through a queue and the CAS holds; across two
   instances there is no queue to share, both reads see the same head, both
   comparisons match, and both publish. The later write wins and the
   earlier writer's commit is unreachable -- while both were told they
   succeeded.

   Asserting the loss rather than the safety is deliberate. If this ever
   starts passing, IPNS grew a conditional publish or the adapter grew a
   real lock, and either way this file should be revisited rather than
   quietly continuing to describe a limit that moved."
  []
  (let [node (ipfs-node)
        writer-a (adapter node)
        writer-b (adapter node)]
    (-> (storage/-compare-and-set-ref! writer-a "main" nil "cid-genesis")
        (.then (fn [_]
                 ;; Both read "cid-genesis" before either publishes.
                 (js/Promise.all
                  #js [(storage/-compare-and-set-ref!
                        writer-a "main" "cid-genesis" "cid-from-a")
                       (storage/-compare-and-set-ref!
                        writer-b "main" "cid-genesis" "cid-from-b")])))
        (.then (fn [[a b]]
                 (expect (and (:published? a) (:published? b))
                         "two writer processes are BOTH told they published")
                 (storage/-read-ref writer-a "main")))
        (.then (fn [head]
                 (expect (= "cid-from-b" (:cid head))
                         "and only the last one survives -- cid-from-a is lost")
                 (expect (nil? (:version head))
                         "the version is nil, not a locally invented counter"))))))

(defn- pin-profile []
  (let [backend (adapter (ipfs-node))]
    (expect (= :single-writer-ref (storage/ref-profile backend))
            "the backend declares single-writer, so the suite will not race it")
    (expect (false? (storage/linearizable? backend))
            "and does not claim linearizability")))

(defn -main [& _]
  (pin-profile)
  (-> (demonstrate-lost-update)
      (.then (fn [_] (contract/verify (adapter (ipfs-node)))))
      (.then (fn [result]
               (println (str "IPFS contract: " (pr-str result)))
               (expect (= :not-claimed (:concurrency result))
                       "the contract reports that concurrency was NOT verified here")))
      (.catch (fn [error]
                (js/console.error (str "FAIL: contract -- " (.-message error)))
                (when-let [data (ex-data error)] (js/console.error (pr-str data)))
                (swap! failures inc)))
      (.then (fn [_]
               (if (zero? @failures)
                 (println "kotobase-storage-ipfs: all green")
                 (println (str "kotobase-storage-ipfs: " @failures " FAILURE(S) above")))
               (.exit js/process (if (zero? @failures) 0 1))))))

(-main)
