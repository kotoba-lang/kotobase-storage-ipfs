(ns kotobase.storage.ipfs
  "IPFS block + single-writer IPNS ref implementation.

  **The single-writer profile is the whole design, not a caveat.** IPNS has
  no compare-and-swap: `name/publish` replaces the record unconditionally.
  What `-compare-and-set-ref!` does here is read the current name, compare,
  and publish -- which is only a CAS because `serialize!` funnels every
  publication through one in-process queue. Two writers in two processes
  both read the same head, both find it matching, and both publish. The
  later one wins and the earlier one's commit becomes unreachable, with
  both told they succeeded.

  So a deployment is correct when, and only when, exactly one process
  publishes each key. `test/run.cljs` demonstrates the loss executably
  rather than describing it, because a limit nobody can reproduce tends to
  get treated as theoretical.

  **Sequence numbers.** Kubo's `name/resolve` returns a path and no record
  metadata, so this adapter's `:version` for a Kubo-backed name is `nil`
  and says so. An earlier version returned a counter held in a JS atom
  inside the client: it started at 0, was never read from any IPNS record,
  and reset on every process restart -- so it reported version 1 for a name
  the network held at sequence 400, and two processes reported the same
  version for different records. A `nil` that means \"this transport does
  not expose one\" is worth more than a number that means nothing."
  (:require [kotobase.storage.core :as storage]))

(defn- serialize! [queue task]
  (let [result (-> @queue (.then task task))]
    (reset! queue (-> result (.then (fn [_] nil) (fn [_] nil))))
    result))

(defrecord IPFSStorage [client ipns-key-fn publish-queue]
  storage/IBlockStore
  (-put-blocks! [_ blocks]
    (-> (mapv
         (fn [{:keys [cid bytes]}]
           (-> ((:put-block! client) cid bytes)
               (.then (fn [stored-cid]
                        (when-not (= cid stored-cid)
                          (throw
                           (ex-info "IPFS returned a different CID"
                                    {:expected cid :actual stored-cid})))
                        cid))))
         blocks)
        clj->js js/Promise.all
        (.then #(vec (array-seq %)))))
  (-get-blocks [_ cids]
    (-> (mapv
         (fn [cid]
           (-> ((:get-block client) cid)
               (.then (fn [bytes] (when bytes [cid bytes])))))
         cids)
        clj->js js/Promise.all
        (.then #(into {} (keep identity (array-seq %))))))

  storage/IRefStore
  (-read-ref [_ name]
    (-> ((:resolve-name client) (ipns-key-fn name) name)
        (.then
         (fn [record]
           (when record
             {:cid (:cid record) :version (:sequence record)})))))
  (-compare-and-set-ref! [this name expected next]
    (serialize!
     publish-queue
     (fn []
       (-> (storage/-read-ref this name)
           (.then
            (fn [current]
              (if (not= expected (:cid current))
                {:published? false :current (:cid current)
                 :version (:version current)}
                (-> ((:publish-name! client)
                     {:key (ipns-key-fn name) :name name :cid next
                      :expected-sequence (:version current)})
                    (.then
                     (fn [record]
                       (if record
                         {:published? true :current next
                          :version (:sequence record)}
                         (-> (storage/-read-ref this name)
                             (.then
                              (fn [winner]
                                {:published? false :current (:cid winner)
                                 :version (:version winner)}))))))))))))))

  storage/IBackendCapabilities
  (-capabilities [_]
    #{:immutable-blocks :cid-addressed-read :conditional-ref
      :single-writer-ref :batch-get :batch-put :distributed-blocks}))

(defn open
  "Open an IPFS/IPNS single-writer backend.

  The injected client supplies Promise-returning `:put-block!`, `:get-block`,
  `:resolve-name`, and `:publish-name!` functions."
  [{:keys [client ipns-key ipns-key-fn]}]
  (doseq [operation [:put-block! :get-block :resolve-name :publish-name!]]
    (when-not (ifn? (get client operation))
      (throw (ex-info "IPFS storage client is incomplete"
                      {:type :kotobase.storage/invalid-configuration
                       :missing operation}))))
  (let [key-fn (or ipns-key-fn (when ipns-key (constantly ipns-key)))]
    (when-not (ifn? key-fn)
      (throw (ex-info "IPFS storage requires :ipns-key or :ipns-key-fn"
                      {:type :kotobase.storage/invalid-configuration})))
    (->IPFSStorage client key-fn (atom (js/Promise.resolve nil)))))

(defn kubo-http-client
  "Build a client for the Kubo HTTP RPC API.

  This is the single-writer profile: every publication for the configured
  IPNS key must pass through one writer process."
  [{:keys [endpoint headers]
    :or {endpoint "http://127.0.0.1:5001"}}]
  (let [endpoint (.replace endpoint #"/+$" "")
        post (fn [path options]
               (js/fetch (str endpoint "/api/v0/" path)
                         (js/Object.assign
                          #js {:method "POST"
                               :headers (clj->js headers)}
                          (clj->js options))))]
    {:put-block!
     (fn [expected-cid bytes]
       (let [form (js/FormData.)]
         (.append form "file" (js/Blob. #js [bytes]))
         (-> (post "block/put?format=dag-cbor&mhtype=sha2-256&pin=true"
                   {:body form})
             (.then
              (fn [response]
                (if (.-ok response)
                  (-> (.json response)
                      (.then (fn [body] (or (.-Key body) expected-cid))))
                  (js/Promise.reject
                   (ex-info "IPFS block put failed"
                            {:status (.-status response)}))))))))
     :get-block
     (fn [cid]
       (-> (post (str "block/get?arg=" (js/encodeURIComponent cid)) nil)
           (.then
            (fn [response]
              (cond
                (.-ok response)
                (-> (.arrayBuffer response) (.then #(js/Uint8Array. %)))
                (= 404 (.-status response)) nil
                :else
                (js/Promise.reject
                 (ex-info "IPFS block get failed"
                          {:cid cid :status (.-status response)})))))))
     :resolve-name
     (fn [key _]
       (-> (post (str "name/resolve?arg=" (js/encodeURIComponent key)) nil)
           (.then
            (fn [response]
              (if-not (.-ok response)
                nil
                (-> (.json response)
                    (.then
                     (fn [body]
                       (let [path (.-Path body)
                             cid (when path (.replace path #"^/ipfs/" ""))]
                         ;; `name/resolve` returns the path and nothing
                         ;; else -- no sequence, no validity, no signer.
                         ;; nil is the honest answer; a locally invented
                         ;; counter would read like record metadata and
                         ;; be unrelated to the record.
                         (when cid {:cid cid :sequence nil}))))))))))
     :publish-name!
     (fn [{:keys [key cid]}]
       ;; Unconditional by construction. `expected-sequence` is accepted by
       ;; the protocol and deliberately unused here: Kubo has nothing to
       ;; condition on, and pretending otherwise is what made the previous
       ;; version look like a CAS.
       (-> (post (str "name/publish?arg=/ipfs/"
                      (js/encodeURIComponent cid)
                      "&key=" (js/encodeURIComponent key))
                 nil)
           (.then
            (fn [response]
              (if (.-ok response)
                {:cid cid :sequence nil}
                (js/Promise.reject
                 (ex-info "IPNS publish failed"
                          {:cid cid :status (.-status response)})))))))}))
