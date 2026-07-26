(ns run
  (:require [kotobase.storage.async-contract :as contract]
            [kotobase.storage.ipfs :as ipfs]))

(defn client []
  (let [blocks (atom {})
        refs (atom {})]
    {:put-block!
     (fn [cid bytes]
       (swap! blocks assoc cid bytes)
       (js/Promise.resolve cid))
     :get-block
     (fn [cid] (js/Promise.resolve (get @blocks cid)))
     :resolve-name
     (fn [_ name] (js/Promise.resolve (get @refs name)))
     :publish-name!
     (fn [{:keys [name cid expected-sequence]}]
       (let [current (get @refs name)]
         (if (not= expected-sequence (:sequence current))
           (js/Promise.resolve nil)
           (let [record {:cid cid
                         :sequence (inc (or (:sequence current) 0))}]
             (swap! refs assoc name record)
             (js/Promise.resolve record)))))}))

(-> (contract/verify
     (ipfs/open {:client (client) :ipns-key "test-writer"}))
    (.then (fn [result] (println "IPFS contract:" (pr-str result))))
    (.catch (fn [error] (js/console.error error) (js/process.exit 1))))
