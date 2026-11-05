(ns jepsen.fiber.rpc
  "A minimal JSON-RPC client for fnn nodes (and, through call-url, the CKB node).
  Fiber and CKB encode amounts and counts as 0x-prefixed hex strings."
  (:require [cheshire.core :as json]
            [clj-http.client :as http]))

(def port 8227)

(defn node-url
  [node]
  (str "http://" (name node) ":" port))

(defn call-url
  "Calls `method` at the JSON-RPC endpoint `url` with the positional `params`,
  returning the result. Throws ex-info with :type :rpc-error when the endpoint
  returns a JSON-RPC error; connection and timeout errors propagate as the
  underlying exceptions."
  [url method params {:keys [timeout] :or {timeout 5000}}]
  (let [body   {:jsonrpc "2.0"
                :id      1
                :method  method
                :params  (vec params)}
        resp   (http/post url
                          {:body               (json/generate-string body)
                           :content-type       :json
                           :accept             :json
                           :socket-timeout     timeout
                           :connection-timeout 2000
                           :throw-exceptions   false})
        parsed (json/parse-string (:body resp) true)]
    (if-let [err (:error parsed)]
      (throw (ex-info (str method ": " (:message err))
                      {:type :rpc-error, :method method, :error err}))
      (:result parsed))))

(defn call
  "Calls `method` on `node` with an optional params map, returning the result.
  Throws ex-info with :type :rpc-error when the node returns a JSON-RPC error;
  connection and timeout errors propagate as the underlying exceptions."
  ([node method]
   (call node method nil))
  ([node method params]
   (call node method params {}))
  ([node method params opts]
   (call-url (node-url node) method (if (nil? params) [] [params]) opts)))

(defn hex->int
  [s]
  (when s
    (bigint (BigInteger. ^String (subs s 2) 16))))

(defn int->hex
  [n]
  (str "0x" (.toString (biginteger n) 16)))
