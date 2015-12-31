(ns sf6.core
  (:require [clojure.data.json :as json]
            [clojure.string :as str]
            [clj-http.client :as client]
            [gniazdo.core :as ws]
            [clojure.core.async
             :as a
             :refer [>! <! >!! <!! go chan buffer close! thread
                     alt! alts! alts!! timeout]])
  (:use [clojure.java.io]
        [clojure.pprint]
        [clojure.tools.namespace.repl :only (refresh)])
  (:gen-class))

(def base-url "https://api.stockfighter.io/ob/api")
(def base-ws-url "wss://api.stockfighter.io/ob/api/ws")
(def api-key (System/getenv "APIKEY"))



(defn- get-raw-tickertape-channel [account venue]
  (let [out  (chan)
        sock (ws/connect
              (str base-ws-url  "/" account "/venues/" venue "/tickertape")
              :on-receive #(>!! out %))]
    out))

(defn- get-fills-channel [account venue sym]
  (let [out  (chan)
        sock (ws/connect
              (str base-ws-url  "/" account "/venues/" venue "/executions/stocks/" sym)
              :on-receive #(>!! out [account %] ))]
    out))

(defn- json-parser
  [in]
  (let [out (chan)]
    (go (while true (>! out (json/read-str (<! in)))))
    out))


(defn- read-response
  "read general responses from stockfighter api"
  [response]
  (update response :body json/read-str))


(defn- try-delete-order
  "tries to delete an order via stockfighter api"
  [venue sym id]
  (let [url (str base-url "/venues/" venue "/stocks/" sym "/orders/" id)]
    (read-response
     (try
       (client/delete url
                      {:headers {"X-Starfighter-Authorization" api-key}})
       (catch Exception ex
         (.getData ex))))))

(defn- get-max-order-id-channel
  [venue sym]
  (let [out (chan)]
    (go (while true
          (let [r (try-delete-order venue sym 1000000000)]
            (if-let [match (and r
                                (= (:status r) 404)
                                (re-find #"on this venue is (\d+)" (get-in r [:body "error"])))]
              (>! out (Integer. (second match)))))))
    out))

(defn- up-counter
  [in]
  (let [out (chan)]
    (go (loop [next-id 1
               max-id (<! in)]
          (if (<= next-id max-id)
            (do (>! out next-id)
                (recur (inc next-id)
                       max-id))
            (recur next-id
                   (<! in)))))
    out))

(defn- add-account-reader
  [in out venue sym]
  (go (while true
        (let [id (<! in)
              r (try-delete-order venue sym id)]
          (if-let [match (and r
                              (= (:status r) 401)
                              (re-find #"account ([A-Z0-9]+)" (get-in r [:body "error"])))]
            (>! out (second match)))))))


(defn- uniques-getter
  [in]
  (let [out (chan)]
    (go (loop [elems #{}]
          (let [elem (<! in)]
            (if (get elems elem)
              (recur elems)
              (do (>! out elem)
                  (recur (conj elems elem)))))))
    out))


(defn- collapse-fills
  [ids-in venue sym]
  (let [out (chan)]
    (go (loop [account-order []
               channel-of    {}]
          ;;ids-in always at head of channel
          (let [channels   (into [ids-in] (map channel-of account-order))      
                [val chan] (alts! channels :priority true)]
            (if (= chan ids-in)
              ;;got a new id
              ;;put new account at the front of the accounts
              ;;associate a new channel for this account
              (recur (into [val] account-order)
                     (assoc channel-of val (get-fills-channel val venue sym))) 
              ;;got a fill from a known id
              (let [[account fill] val]
                (>! out fill)                            ;;place fill on out channel
                ;;put account we got a fill for at end of accounts
                ;;channel-of left alone
                (recur (conj (into [] (remove #(= account %) account-order))   
                             account)
                       channel-of                                    
                   ))))))
    out))



(defn- update-status
  [prev-status fill]
  ;;(pprint fill)
  (let [fills  (get-in fill ["order" "fills"])
        direction (get-in fill ["order" "direction"])
        pq-sum (reduce + (map #(* (% "price") (% "qty")) fills))
        q-sum  (reduce + (map #(% "qty") fills))
        d-cash (if (= direction "buy") (* -1 pq-sum) pq-sum)
        d-qty  (if (= direction "buy") q-sum         (* -1 q-sum))]
    (if (nil? prev-status)
      {:cash    d-cash
       :qty     d-qty
       :abs-qty q-sum}
      (do ;;(println "getting a status weve seen before")
          ;;(pprint prev-status)
          (-> prev-status
              (update :cash + d-cash)
              (update :qty  + d-qty)
              (update :abs-qty + q-sum))))))

(defn- get-market-price
  [last-market-price res]
  (let [bid (get-in res ["quote" "bid"])
        ask (get-in res ["quote" "ask"])]
    (if (and bid ask)
      (* 0.5 (+ bid ask))
      last-market-price)))

(defn- get-market-price-chan
  [account venue]
  (let [ticker-chan (-> (get-raw-tickertape-channel account venue)
                        json-parser)
        out (chan)]
    (go (loop [market-price nil]
          (let [new-price (get-market-price market-price (<! ticker-chan))]
            (if new-price (>! out new-price)) 
            (recur new-price))))
    out))

(defn- get-navs
  [status-of market-price]
  ;;(pprint status-of)
  ;;(pprint market-price)
  (cond
    (nil? market-price) "no market price"
    (nil? status-of)    "no statuses"
    :else               (reduce
                         (fn [a-map [k v]]
                           (let [nav     (+ (:cash v) (* (:qty v) market-price) )
                                 nav-eff (/ nav (:abs-qty v))
                                 ]
                             (assoc a-map k (assoc v :nav nav :nav-eff nav-eff
                                                   ))))
                         {}
                         status-of)))

(defn- track-navs
  [fills-chan ticker-chan]
  (let [out (chan)]
    (go (loop [status-of {}
               market-price nil]
          (let [timeout-chan (timeout 10)]
            ;;(println "about to send back")
            (alt!
              [[out (get-navs status-of market-price)]] :sent
              timeout-chan                              :timeout)
            ;;(pprint (get-navs status-of market-price))
            ;;(println "about to hit fills or tickers")
            ;;(pprint status-of)
            (let [[new-status-of new-market-price]
                  (alt!
                    fills-chan   ([res]
                                  ;;(println "hit fills")
                                  ;;(println "account: " (res "account"))
                                  [(update status-of (res "account") update-status res) market-price])
                    ticker-chan  ([res]
                                  ;;(println "hit ticker")
                                  [status-of (get-market-price market-price res)] )
                    :priority    true
                    )]
              (recur new-status-of new-market-price)))))
    out))

(defn- get-ticks
  "convert string timestamp to ticks"
  [ts]
  (when ts
    (-> ts
        clojure.instant/read-instant-timestamp
        .getTime)))


(defn- summarize-tick
  [tick]
  (when (and tick (tick "ok"))
    (let [q (tick "quote")]
      {:bid (q "bid") :ask (q "ask") :time (get-ticks (q "quoteTime"))}
      )))

(defn- summarize-fill
  [fill]
  (when (and fill (fill "ok"))
    (let [account (fill "account")
          direction (get-in fill ["order" "direction"])
          f (get-in fill ["order" "fills"] )]
      (into []
            (map (fn [m]
                   {:price (m "price")
                    :qty (m "qty")
                    :time (get-ticks (m "ts"))
                    :account account
                    :direction (if (= direction "buy") :buy :sell)
                    })
                 f)))))



(defn- merge-fills-and-ticks
  [fills-chan ticker-chan]
  (let [out (chan)]
    (go (while true
          (>! out
              (alt!
                fills-chan   ([res] [:fill (summarize-fill res)])
                ticker-chan  ([res] [:tick (summarize-tick res)])
                :priority true))))
    out))

(defn- echo-chan
  [in]
  (go (while true
        (println (<! in)))))



(defn making-amends
  [account venue sym]
  (let [order-id-channel (-> (get-max-order-id-channel venue sym)
                             up-counter)
        accounts-channel (chan)
        fills-channel (-> accounts-channel
                          uniques-getter
                          (collapse-fills venue sym)
                          json-parser)
        ticker-channel (-> (get-raw-tickertape-channel account venue)
                           json-parser)
        ;;navs-chan      (track-navs fills-channel ticker-channel)
        merge-chan      (merge-fills-and-ticks fills-channel ticker-channel)
        ]
    (add-account-reader order-id-channel accounts-channel venue sym)
    (add-account-reader order-id-channel accounts-channel venue sym)
    (add-account-reader order-id-channel accounts-channel venue sym)
    (add-account-reader order-id-channel accounts-channel venue sym)
    (add-account-reader order-id-channel accounts-channel venue sym)

    
    (echo-chan merge-chan) 
    ))


