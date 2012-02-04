(ns vhector.internal.hector
  (:use
    [vhector.internal.util :only [single?]])
  (:import 
    [me.prettyprint.hector.api.factory HFactory]
    [me.prettyprint.cassandra.serializers StringSerializer]
    [java.nio.ByteBuffer]
    [java.util.Arrays]))

(def DEFAULT_MAX_ROWS 1000000)
(def DEFAULT_MAX_COLS Integer/MAX_VALUE)

(defn encode [data]
  (cond
    (or (nil? data) (= :FROM-FIRST data) (= :TO-LAST data)) nil
    (coll? data) (map encode data)
    :default (pr-str data)))

(defn decode [s]
  (with-in-str (String. (.getBytes s) "UTF-8") (read)))
 
(defn expand-tree [t]
  (if (map? t)
    (for [[k v] t, w (expand-tree v)]
      (cons k w))
    (list (list t))))

(def se (StringSerializer/get))

(defn create-keyspace [cluster-name host port ks]
  (let [cluster (HFactory/getOrCreateCluster cluster-name (str host ":" port))]
    (HFactory/createKeyspace ks cluster)))

(defn init [cluster-name host port ks]
  (def ^:dynamic *keyspace* (create-keyspace cluster-name host port ks)))

(defn convert-slice [slice]
  (let [cols (.getColumns slice)
        cols-seq (iterator-seq (.iterator cols))]
    (reduce
      (fn [m col]
        (assoc m
          (decode (.getName col))
          (decode (.getValue col))))
      {}
      cols-seq)))

(defn convert-super-slice [super-slice]
  (let [super-cols (.getSuperColumns super-slice)
        super-cols-seq (iterator-seq (.iterator super-cols))]
    (reduce
      (fn [m super-col]
        (assoc m
          (decode (.getName super-col))
          (convert-slice super-col)))
      {}
      super-cols-seq)))

(defn extract-columns [row]
  (convert-slice (.getColumnSlice row)))

(defn extract-columns-with-super [super row]
  {super (convert-slice (.getColumnSlice row))})

(defn extract-super-columns [row]
  (convert-super-slice (.getSuperSlice row)))

(defn extract-rows 
  ([rows]
    (extract-rows rows extract-columns))
  ([rows extract-fn]
    (let [rows-seq (iterator-seq (.iterator rows))]
      (reduce
        (fn [m row]
          (assoc m 
                 (decode (.getKey row))
                 (extract-fn row)))
        {}
        rows-seq))))

(defn set-column-names
  [query crit]
  (let [arr (if (vector? crit) crit [crit])]
    (doto query (.setColumnNames (to-array (encode arr))))))

(defn set-super-column 
  [query crit]
  (doto query (.setSuperColumn (encode crit))))

(defn is-reversed
  [start stop raw-stop]
  (if (= :FROM-FIRST raw-stop)
    true
    (if-let [real-stop stop]
      (pos? (compare start real-stop))
      false))) ; consider that nil as stop means normal order 

(defn apply-range 
  [query crit max-res]
  (if (map? crit)
    (let [crit-start (first (keys crit))
          crit-stop (get crit crit-start)
          encoded-start (encode crit-start)
          encoded-stop (encode crit-stop)
          reversed  (is-reversed encoded-start encoded-stop crit-stop)]
      (doto query (.setRange encoded-start encoded-stop reversed max-res)))
    (set-column-names query crit)))

(defn execute-query
  [query cf ks]
  (let [ks-vec (if (vector? ks) ks [ks])]
    (doto query 
      (.setColumnFamily cf)
      (.setKeys (to-array (encode ks-vec))))
    (-> query .execute .get)))

(defn read-rows
  ([cf ks cols]
    (read-rows cf ks cols DEFAULT_MAX_COLS))
  ([cf ks cols max-cols]
    (let [query (HFactory/createMultigetSliceQuery *keyspace* se se se)]
      (apply-range query cols max-cols)
      (extract-rows (execute-query query cf ks)))))

(defn read-row-super
  ([cf ks super cols]
    (read-row-super cf ks super cols DEFAULT_MAX_COLS))
  ([cf ks super cols max-cols]
    (let [query (HFactory/createMultigetSubSliceQuery *keyspace* se se se se)]
      (apply-range query cols max-cols)
      (doto query (.setSuperColumn (encode super)))
      (extract-rows (execute-query query cf ks) (partial extract-columns-with-super super))))) 

(defn read-rows-super
  ([cf ks sups cols]
    (read-rows-super cf ks sups cols DEFAULT_MAX_COLS))
  ([cf ks sups cols max-sups]
    (let [query (HFactory/createMultigetSuperSliceQuery *keyspace* se se se se)]
      (apply-range query sups max-sups)
      (extract-rows (execute-query query cf ks) extract-super-columns)))) 

(defn prep-range-query
  [cf ks max-rows query]
  (let [key-start (first (keys ks))
        key-stop (get ks key-start)]
    (doto query 
      (.setColumnFamily cf)
      (.setKeys (encode key-start) (encode key-stop))
      (.setRowCount max-rows))))

(defn read-range-rows
  ([cf ks cols]
    (read-range-rows cf ks cols DEFAULT_MAX_ROWS))
  ([cf ks cols max-rows]
    (read-range-rows cf ks cols max-rows DEFAULT_MAX_COLS))
  ([cf ks cols max-rows max-cols]
    (let [q (HFactory/createRangeSlicesQuery *keyspace* se se se)
          range-slices-query (prep-range-query cf ks max-rows q)]
      (apply-range range-slices-query cols max-cols)
      (-> range-slices-query .execute .get extract-rows))))

(defn read-range-rows-super
  ([cf ks sups cols]
    (read-range-rows-super cf ks sups cols DEFAULT_MAX_ROWS))
  ([cf ks sups cols max-rows]
    (read-range-rows-super cf ks sups cols max-rows DEFAULT_MAX_COLS))
  ([cf ks sups cols max-rows max-cols]
    (if (single? sups)
      (let [q (HFactory/createRangeSubSlicesQuery *keyspace* se se se se)
            range-slices-query (prep-range-query cf ks max-rows q)]
        (doto range-slices-query (.setSuperColumn (encode sups)))
        (apply-range range-slices-query cols max-cols)
        (extract-rows (-> range-slices-query .execute .get) (partial extract-columns-with-super sups)))
      (let [q (HFactory/createRangeSuperSlicesQuery *keyspace* se se se se)
            range-slices-query (prep-range-query cf ks max-rows q)]
        (apply-range range-slices-query sups max-cols)
        (extract-rows (-> range-slices-query .execute .get) extract-super-columns)))))

(defn add-insertion 
  ([mutator cf k col v]
    (add-insertion mutator cf nil k col v))
  ([mutator cf super k col v]
    (let [stringcol (HFactory/createColumn (encode col) (encode v) se se)]
      (if (nil? super)
        (.addInsertion mutator (encode k) cf stringcol)
        (let [super-col (HFactory/createSuperColumn (encode k) [stringcol] se se se)]
          (.addInsertion mutator (encode super) cf super-col))))))

(defn add-deletion 
  ([mutator cf ks]
    (if-not (vector? ks)
      (add-deletion mutator cf [ks])
      (doseq [k ks] (.addDeletion mutator (encode k) cf))))
  ([mutator cf k cols]
    (if-not (vector? cols)
      (add-deletion mutator cf k [cols])
      (doseq [col cols] (.addDeletion mutator (encode k) cf (encode col) se))))
  ([mutator cf k super cols]
    (if-not (vector? cols)
      (add-deletion mutator cf k super [cols])
      (doseq [col-name cols] 
        (let [stringcol (HFactory/createColumn (encode col-name) "" se se) ; value is not used but must not be null
              super-col (HFactory/createSuperColumn (encode super) [stringcol] se se se)]
          (.addSubDelete mutator (encode k) cf super-col))))))

(defn process-tree [tree func]
  (let [mutator (HFactory/createMutator *keyspace* se)
        records (expand-tree tree)]
    (doseq [record records]
      (apply func mutator record))
    (.execute mutator)))

(defn internal-insert! [tree]
  (process-tree tree add-insertion))

(defn internal-delete! [tree]
  (process-tree tree add-deletion))
