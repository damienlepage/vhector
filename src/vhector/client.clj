; Copyright (c) 2011 Damien Lepage

; Permission is hereby granted, free of charge, to any person obtaining a copy
; of this software and associated documentation files (the "Software"), to deal
; in the Software without restriction, including without limitation the rights
; to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
; copies of the Software, and to permit persons to whom the Software is
; furnished to do so, subject to the following conditions:

; The above copyright notice and this permission notice shall be included in
; all copies or substantial portions of the Software.

; THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
; IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
; FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
; AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
; LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
; OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
; THE SOFTWARE.

(ns vhector.client
  #^{:author "Damien Lepage",
     :doc "Wrapper for Hector, java library for Cassandra"}
    (:use 
      [vhector.internal.util :only [enmap demap]]
      [vhector.internal.dispatch :only [dispatch-select]]
      [vhector.internal.hector :only [init, create-keyspace, internal-insert!, internal-delete!]]))

;TODO support UUID
;TODO support secondary column indexes: IndexedSlicesQuery

(defn connect!
  "Connect to Cassandra. Usage:
  (connect! \"Test Cluster\" \"localhost\" 9160 \"Keyspace1\")"
  [cluster-name host port ks]
  (init cluster-name host port ks))

(defmacro with-cass
  "Override the active connection in the enclosing scope."
  [cluster-name host port ks & body]
  `(do
     (binding [vhector.internal.hector/*keyspace* (create-keyspace ~cluster-name ~host ~port ~ks)]
       ~@body)))

(defn insert!
  "Insert data into Cassandra, for both standard and super column families.
  The following formats are supported:

  Standard column families:
  ------------------------------------------------------
  (insert! \"Standard1\" \"Aldebaran\" :constellation \"Taurus\")
  (insert! \"Standard1\" \"Aldebaran\" {:constellation \"Taurus\", :distance 65})
  (insert! \"Standard1\" {\"Aldebaran\" {:constellation \"Taurus\", :distance 65}
                        \"Rigel\" {:constellation \"Orion\", :distance 772.51, :mass 17}})
  (insert! {\"Standard1\" {\"Aldebaran\" {:constellation \"Taurus\", :distance 65}
                         \"Rigel\" {:constellation \"Orion\", :distance 772.51, :mass 17}}
            \"Standard2\" {\"Mars\" {:mass 0.107}
                         \"Jupiter\" {:mass 317.8, :radius 10.517}}})

  Super column families:
  ------------------------------------------------------
  (insert! \"Super1\" \"Saturn\" \"Titan\" :radius 2576)
  (insert! \"Super1\" \"Saturn\" \"Titan\" {:radius 2576, :temperature 93.7})
  (insert! \"Super1\" \"Saturn\" {\"Titan\" {:radius 2576, :temperature 93.7}
                             \"Tethys\" {:radius 533, :temperature 86, :semi-major-axis 294619}})
  (insert! \"Super1\" {\"Saturn\" {\"Titan\" {:radius 2576, :temperature 93.7}
                             \"Tethys\" {:radius 533, :temperature 86, :semi-major-axis 294619}}
                     \"Earth\" {\"Moon\" {:radius 1737.10}}})
  (insert! {\"Super1\" {\"Saturn\" {\"Titan\" {:radius 2576, :temperature 93.7}
                             \"Tethys\" {:radius 533, :temperature 86, :semi-major-axis 294619}}
                     \"Earth\" {\"Moon\" {:radius 1737.10}}}
            \"Super2\" {\"Sun\" {\"Halley\" {:mass \"2.2*1014\"}}}})"
  ([tree]
    (internal-insert! tree))
  ([arg & args]
    (insert! (apply enmap arg args))))

(defn select
  "Select data from Cassandra, from both standard and super column families.
  Here is the list of arguments and their accepted formats:

  cf: column-family, String
  ks: keys of the records to look-up, string, vector or map (see below)
  super (optional): super columns to look-up, string, vector or map (see below)
  cols: columns to look-up, string, vector or map (see below)
  max-rows (optional): maximum number of rows to return (only when range of keys is used)
  max-super (optional): maximum number of super-columns to return (only when range of super is used)
  max-cols (optional): maximum number of columns to return (only when range of columns is used)

  The returned value can be a single element (when one key and one column are provided) or
  nested maps in all other cases, with the top level being the highest one needing discrimination.

  Keys and Columns
  ------------------------------------------------------
  Keys and Columns arguments support 3 types:
  string: one key or one column
  vector: list of keys/columns
  map: range of keys/columns with support for unlimited end. 
    E.g.
    {:start :stop} => all keys/columns between :start and :stop (inclusive)
    {:start nil} => all keys/columns from :start to the end
    {nil :stop} => all keys/columns from the beginning to :stop
    {} or {nil nil} => all keys/columns

  NOTE: range of keys shouldn't be used if Cassandra is configured with the default RandomPartitioner.
  They become useful only with the OrderPreservingPartitioner.
  
  Examples with Standard column families:
  ------------------------------------------------------
  =>(select \"Standard1\" \"Rigel\" :distance) ; one row, one column
  772.51
  =>(select \"Standard1\" \"Rigel\" {}) ; one row, all columns
  {:radius 78, :mass 17, :distance 772.51, :constellation \"Orion\"}
  =>(select \"Standard1\" \"Rigel\" [:distance :mass]) ; one row, list of columns
  {:mass 17, :distance 772.51}
  =>(select \"Standard1\" \"Rigel\" {:constellation :mass}) ; one row, range of columns
  {:mass 17, :distance 772.51, :constellation \"Orion\"}
  =>(select \"Standard1\" {} {} 3 1) ; all keys and columns with limits of 3 rows and 1 col
  {\"Rigel\" {:constellation \"Orion\"}, \"Mu Cephei\" {:constellation \"Cepheus\"}, \"Aldebaran\" {:constellation \"Taurus\"}}
  =>(select \"Standard1\" [\"Rigel\" \"Aldebaran\"] {}) ; list of rows, all columns
  {\"Rigel\" {:radius 78, :mass 17, :distance 772.51, :constellation \"Orion\"}, \"Aldebaran\" {:distance 65, :constellation \"Taurus\"}}
  =>(select \"Standard1\" [\"Rigel\" \"Aldebaran\"] [:distance :mass]) ; list of rows, list of columns
  {\"Rigel\" {:mass 17, :distance 772.51}, \"Aldebaran\" {:distance 65}}

  Examples with Super column families:
  ------------------------------------------------------
  => (select \"Super1\" \"Saturn\" \"Titan\" :radius) ; one key, one super, one col
  2576
  => (select \"Super1\" \"Saturn\" \"Titan\" [:radius :temperature]) ; one key, one super, list of cols
  {:temperature 93.7, :radius 2576}
  => (select \"Super1\" \"Saturn\" \"Tethys\" {}) ; one key, one super, all cols
  {:temperature 86, :semi-major-axis 294619, :radius 533}
  => (select \"Super1\" \"Saturn\" {\"Abc\" \"Titan\"} {} 1) ; one key, range of super, all cols with limit of 1 super
  {\"Tethys\" {:temperature 86, :semi-major-axis 294619, :radius 533}}
  => (select \"Super1\" [\"Earth\" \"Saturn\"] \"Tethys\" {:aaa :sss}) ; list of keys, one super, range of cols with limit of 1 col 
  {\"Saturn\" {\"Tethys\" {:semi-major-axis 294619, :radius 533}}, \"Earth\" {\"Tethys\" {}}}

  All columns {} must be selected when using a list or a range of super columns.

  Note about performance: setting a very high limit on number of rows seems to have a big performance 
  impact on Cassandra. Therefore, when not specified, the default limit of rows is 1 million. The 
  default limit of cols is Integer/MAX_VALUE.

  More examples are available in client_test.clj"
  {:arglists '([cf ks super* cols max-rows* max-super* max-cols*])}
  [& args]
  (let [m-res (apply dispatch-select args)]
    (apply demap m-res args)))

(defn delete!
  "Delete data from Cassandra, for both standard and super column families.
  The following formats are supported:

  Standard column families:
  ------------------------------------------------------
  (delete! \"Standard1\" \"Aldebaran\") ; delete 1 key
  (delete! \"Standard1\" [\"Aldebaran\" \"Rigel\"]) ; delete many keys
  (delete! \"Standard1\" \"Aldebaran\" :constellation) ; delete 1 column
  (delete! \"Standard1\" \"Aldebaran\" [:constellation :distance]) ; delete many columns under 1 key
  (delete! \"Standard1\" {\"Aldebaran\" [:constellation :distance]
                        \"Rigel\" [:constellation :distance :mass]}) ; delete many columns under many keys 
  (delete! {\"Standard1\" [\"Aldebaran\" \"Rigel\"]
            \"Standard2\" [\"Mars\" \"Jupiter\"]}) ; delete many keys under many CF
  (delete! {\"Standard1\" \"Aldebaran\"
            \"Standard2\" \"Mars\"}) ; delete 1 key under many CF
  (delete! {\"Standard1\" \"Aldebaran\"
            \"Standard2\" [\"Mars\" \"Jupiter\"]}) ; delete 1 key or many keys under many CF
  (delete! {\"Standard1\" {\"Aldebaran\" [:constellation :distance]
                         \"Rigel\" [:constellation :distance :mass]}
            \"Standard2\" {\"Mars\" :mass
                         \"Jupiter\" [:mass :radius]}}) ; delete many columns under many keys under many CF

  Super column families:
  ------------------------------------------------------
  (delete! \"Super1\" \"Saturn\") ; delete 1 key
  (delete! \"Super1\" [\"Saturn\" \"Earth\"]) ; delete many keys
  (delete! \"Super1\" \"Saturn\" \"Titan\" :radius) ; delete 1 column
  (delete! \"Super1\" \"Saturn\" \"Titan\" [:radius :temperature]) ; delete many columns
  (delete! \"Super1\" \"Saturn\" {\"Titan\" [:radius :temperature]
                                \"Tethys\" [:radius :temperature :semi-major-axis]) ; delete many columns under many super columns
  (delete! \"Super1\" {\"Saturn\" {\"Titan\" [:radius :temperature]
                                \"Tethys\" [:radius :temperature :semi-major-axis]} 
                      \"Earth\" {\"Moon\" :radius}}) ; delete many columns under many super columns under many keys
  (delete! {\"Super1\" {\"Saturn\" {\"Titan\" [:radius :temperature]
                                \"Tethys\" [:radius :temperature :semi-major-axis]} 
                      \"Earth\" {\"Moon\" :radius}} 
          \"Super2\" {\"Sun\" {\"Halley\" :mass}}}) ; delete many columns under many super columns under many keys
  (delete! {\"Super1\" [\"Saturn\" \"Earth\"]
          \"Super2\" {\"Sun\" {\"Halley\" :mass}}}) ; delete a mix of whole keys and specific columns

  NOTE: it is not possible to delete all columns for a given super column."
  ([tree]
    (internal-delete! tree))
  ([arg & args]
    (delete! (apply enmap arg args))))
