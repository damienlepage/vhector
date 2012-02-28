(ns vhector.internal.hector
  (:use
    [vhector.internal.util :only [single?]])
  (:import 
    [me.prettyprint.hector.api.factory HFactory]
    [me.prettyprint.cassandra.serializers BytesArraySerializer]
    [me.prettyprint.cassandra.model CqlQuery]
    [java.io DataOutputStream ByteArrayOutputStream DataInputStream ByteArrayInputStream]))

(def DEFAULT_MAX_ROWS 1000000)
(def DEFAULT_MAX_COLS Integer/MAX_VALUE)
(def LONG_SUFFIX "?long")
(def DOUBLE_SUFFIX "?double")
(def END_SUFFIX "?zzz")

(def se (BytesArraySerializer/get))

(defn create-keyspace [cluster-name host port ks]
  (let [cluster (HFactory/getOrCreateCluster cluster-name (str host ":" port))]
    (HFactory/createKeyspace ks cluster)))

(defn init [cluster-name host port ks typed-columns]
  (def ^:dynamic *keyspace* (create-keyspace cluster-name host port ks))
  (def ^:dynamic *typed-columns* typed-columns))

(defn longable? [x]
  (or (instance? Short x) (instance? Integer x) (instance? Long x)))

(defn doublable? [x]
  (or (instance? Double x) (instance? Float x)))

(defn get-data-suffix [x]
  (cond
    (longable? x) LONG_SUFFIX
    (doublable? x) DOUBLE_SUFFIX
    :default nil))

(defn encode-number [x conv-fct]
  (let [baos (ByteArrayOutputStream.)
        dos (DataOutputStream. baos)]
    (conv-fct x dos)
    (.toByteArray baos)))

(defn encode-string 
  ([x] (encode-string x nil))
  ([x suffix]
    (let [s (str x suffix)
          s2 (if (keyword? x) s (pr-str s))]
      (.getBytes s2 "UTF-8"))))

(defn encode 
  ([data] (encode data nil))
  ([data suffix]
    (cond
      (or (nil? data) (= :FROM-FIRST data) (= :TO-LAST data)) nil
      (coll? data) (map encode data)
      (and *typed-columns* (doublable? data)) (encode-number data #(.writeDouble %2 (double %1)))
      (and *typed-columns* (longable? data)) (encode-number data #(.writeLong %2 (long %1))) 
      :default (encode-string data suffix))))

(defn decode-number [bytes-arr conv-fct]
  (when-not 
    (nil? bytes-arr)
    (let [bais (ByteArrayInputStream. bytes-arr)
          dis (DataInputStream. bais)]
      (conv-fct dis))))

(defn to-string [bytes-arr]
  (when-not 
    (nil? bytes-arr)
    (with-in-str (String. bytes-arr "UTF-8") (read))))

(defn decode 
  ([bytes-arr] (decode bytes-arr nil))
  ([bytes-arr suffix]
    (cond
      (= suffix LONG_SUFFIX) (decode-number bytes-arr #(.readLong %))
      (= suffix DOUBLE_SUFFIX) (decode-number bytes-arr #(.readDouble %))
      :default (to-string bytes-arr))))
 
(defn expand-tree [t]
  (if (map? t)
    (for [[k v] t, w (expand-tree v)]
      (cons k w))
    (list (list t))))

(defn get-suffix-from-col [col]
  (let [s (str col)
        idx (.indexOf s "?")]
    (if (pos? idx)
      (.substring s idx)
      s)))

(defn remove-suffix [col]
  (let [s (str col)
        idx (.indexOf s "?")
        new-col (if (pos? idx) (.substring s 0 idx) s)]
    (if (keyword? col)
      (with-in-str new-col (read))
      new-col)))

(defn convert-slice [slice]
  (let [cols (.getColumns slice)
        cols-seq (iterator-seq (.iterator cols))]
    (reduce
      (fn [m col]
        (let [full-col-name (decode (.getName col))
              suffix (get-suffix-from-col full-col-name)
              col-name (remove-suffix full-col-name)]
          (if 
            (and (not= "KEY" col-name) (nil? (m col-name))) ; make sure we don't overwrite due to typed-columns
            (assoc m
                   col-name
                   (decode (.getValue col) suffix))
            m)))
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
          nillify-special #(if (or (= :FROM-FIRST %) (= :TO-LAST %)) nil %)
          crit-start2 (nillify-special crit-start)
          crit-stop2 (nillify-special crit-stop)
          reversed  (is-reversed crit-start2 crit-stop2 crit-stop)
          encoded-start (if reversed (encode crit-start END_SUFFIX) (encode crit-start)) ; make sure typed cols are included in the 
          encoded-stop (if reversed (encode crit-stop) (encode crit-stop END_SUFFIX))]   ; range by appending ?zzz
      (doto query (.setRange encoded-start encoded-stop reversed max-res)))
    (set-column-names query crit)))

(defn execute-query
  [query cf ks]
  (let [ks-vec (if (vector? ks) ks [ks])]
    (doto query 
      (.setColumnFamily cf)
      (.setKeys (to-array (encode ks-vec))))
    (-> query .execute .get)))

(defn concat-suffix [x suffix] 
  (let [s (str x suffix)]
    (if (keyword? x)
      (with-in-str s (read))
      s)))

(defn expand-possible-cols [cols]
  (if-not (map? cols)
    (apply 
      vector
      (mapcat
        #(vector % (concat-suffix % %2) (concat-suffix % %3)) 
        (if (vector? cols) cols [cols]) 
        (repeat LONG_SUFFIX)
        (repeat DOUBLE_SUFFIX)))
    cols))

(defn read-rows
  ([cf ks cols]
    (read-rows cf ks cols DEFAULT_MAX_COLS))
  ([cf ks cols max-cols]
    (let [cols+ (expand-possible-cols cols)
          query (HFactory/createMultigetSliceQuery *keyspace* se se se)]
      (apply-range query cols+ max-cols)
      (extract-rows (execute-query query cf ks)))))

(defn read-row-super
  ([cf ks super cols]
    (read-row-super cf ks super cols DEFAULT_MAX_COLS))
  ([cf ks super cols max-cols]
    (let [cols+ (expand-possible-cols cols)
          query (HFactory/createMultigetSubSliceQuery *keyspace* se se se se)]
      (apply-range query cols+ max-cols)
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
    (let [cols+ (expand-possible-cols cols)
          q (HFactory/createRangeSlicesQuery *keyspace* se se se)
          range-slices-query (prep-range-query cf ks max-rows q)]
      (apply-range range-slices-query cols+ max-cols)
      (-> range-slices-query .execute .get extract-rows))))

(defn read-range-rows-super
  ([cf ks sups cols]
    (read-range-rows-super cf ks sups cols DEFAULT_MAX_ROWS))
  ([cf ks sups cols max-rows]
    (read-range-rows-super cf ks sups cols max-rows DEFAULT_MAX_COLS))
  ([cf ks sups cols max-rows max-cols]
    (if (single? sups)
      (let [cols+ (expand-possible-cols cols)
            q (HFactory/createRangeSubSlicesQuery *keyspace* se se se se)
            range-slices-query (prep-range-query cf ks max-rows q)]
        (doto range-slices-query (.setSuperColumn (encode sups)))
        (apply-range range-slices-query cols+ max-cols)
        (extract-rows (-> range-slices-query .execute .get) (partial extract-columns-with-super sups)))
      (let [q (HFactory/createRangeSuperSlicesQuery *keyspace* se se se se)
            range-slices-query (prep-range-query cf ks max-rows q)]
        (apply-range range-slices-query sups max-cols)
        (extract-rows (-> range-slices-query .execute .get) extract-super-columns)))))

(defn add-insertion 
  ([mutator cf k col v]
    (add-insertion mutator cf nil k col v))
  ([mutator cf super k col v]
    (let [encoded-col (encode col (get-data-suffix v))
          stringcol (HFactory/createColumn encoded-col (encode v) se se)]
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
      (let [cols+ (expand-possible-cols cols)]
        (doseq [col cols+] (.addDeletion mutator (encode k) cf (encode col) se)))))
  ([mutator cf k super cols]
    (if-not (vector? cols)
      (add-deletion mutator cf k super [cols])
      (let [cols+ (expand-possible-cols cols)]
        (doseq [col-name cols+] 
          (let [stringcol (HFactory/createColumn (encode col-name) (encode-string "") se se) ; value is not used but must not be null
                super-col (HFactory/createSuperColumn (encode super) [stringcol] se se se)]
            (.addSubDelete mutator (encode k) cf super-col)))))))

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

(defn build-cql-cols [cols]
  (cond 
    (= cols [:COUNT]) "1" ; count(*) => https://github.com/rantav/hector/issues/428
    (empty? cols) "*"
    :default (str "'" (reduce str (interpose "', '" (expand-possible-cols cols))) "'")))

(defn escape-cql 
  ([x] (escape-cql x nil))
  ([x suffix] 
    (cond
      (doublable? x) x
      (longable? x) x
      (string? x) (str "'" (pr-str (str x suffix)) "'") 
      :default (str "'" x suffix "'")))) ; keyword 

(defn to-cql-crit [where m]
  (let [operator (.substring (str where) 6)] ; e.g. :WHERE>= gives operator >=
    (apply 
      vector
      (for [[k v] m] 
        (let [encoded-k (escape-cql k (get-data-suffix v))
              encoded-v (escape-cql v)]
          (str encoded-k " " operator " " encoded-v))))))

(defn build-cql-where-clauses [cql-args]
  (when-not 
    (empty? cql-args)
    (concat 
      (to-cql-crit (first cql-args) (second cql-args)) 
      (build-cql-where-clauses (nnext cql-args)))))

(defn build-cql-where-string [clauses]
  (when-not (empty? clauses)
    (let [and-clauses (interpose " AND " clauses)] 
      (str " WHERE " (reduce str and-clauses)))))

(defn build-cql-query [cf cols & cql-args]
  (let [cols-cql (build-cql-cols cols)
        clauses-cql (build-cql-where-clauses cql-args)
        where-cql (build-cql-where-string clauses-cql)
        cql (str "SELECT " cols-cql " FROM " cf where-cql )]
    ;(println "CQL=" cql)
    cql))

(defn cql [cf cols & cql-args]
  (let [query (CqlQuery. *keyspace* se se se)
        _ (doto query (.setQuery (apply build-cql-query cf cols cql-args)))
        res (-> query .execute .get)]
    (when res
      (if (= cols [:COUNT])
        (.getCount res) ; .getAsCount? https://github.com/rantav/hector/issues/428
        (extract-rows res)))))