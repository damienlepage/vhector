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
  ([cluster-name host port ks]
    (connect! cluster-name host port ks true))
  ([cluster-name host port ks typed-columns]
    (init cluster-name host port ks typed-columns)))

(defmacro with-typed-columns
  "Override the typed-columns flag in the enclosing scope."
  [typed-columns & body]
  `(do
     (binding [vhector.internal.hector/*typed-columns* typed-columns]
       ~@body)))

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
  (insert! \"Stars\" \"Aldebaran\" :constellation \"Taurus\")
  (insert! \"Stars\" \"Aldebaran\" {:constellation \"Taurus\", :distance 65})
  (insert! \"Stars\" {\"Aldebaran\" {:constellation \"Taurus\", :distance 65}
                        \"Rigel\" {:constellation \"Orion\", :distance 772.51, :mass 17}})
  (insert! {\"Stars\" {\"Aldebaran\" {:constellation \"Taurus\", :distance 65}
                         \"Rigel\" {:constellation \"Orion\", :distance 772.51, :mass 17}}
            \"Planets\" {\"Mars\" {:mass 0.107}
                         \"Jupiter\" {:mass 317.8, :radius 10.517}}})

  Super column families:
  ------------------------------------------------------
  (insert! \"Moons\" \"Saturn\" \"Titan\" :radius 2576)
  (insert! \"Moons\" \"Saturn\" \"Titan\" {:radius 2576, :temperature 93.7})
  (insert! \"Moons\" \"Saturn\" {\"Titan\" {:radius 2576, :temperature 93.7}
                             \"Tethys\" {:radius 533, :temperature 86, :semi-major-axis 294619}})
  (insert! \"Moons\" {\"Saturn\" {\"Titan\" {:radius 2576, :temperature 93.7}
                             \"Tethys\" {:radius 533, :temperature 86, :semi-major-axis 294619}}
                     \"Earth\" {\"Moon\" {:radius 1737.10}}})
  (insert! {\"Moons\" {\"Saturn\" {\"Titan\" {:radius 2576, :temperature 93.7}
                             \"Tethys\" {:radius 533, :temperature 86, :semi-major-axis 294619}}
                     \"Earth\" {\"Moon\" {:radius 1737.10}}}
            \"Comets\" {\"Sun\" {\"Halley\" {:mass \"2.2*1014\"}}}})"
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
    {:stop :start} => all keys/columns between :start and :stop in reverse order
    {:start nil} or {:start :TO-LAST} => all keys/columns from :start to the end
    {nil :stop} or {:FROM-FIRST :stop} => all keys/columns from the beginning to :stop
    {:stop :FROM-FIRST} => all keys/columns from the beginning to :stop in reverse order
    {} or {nil nil} or {:FROM-FIRST :TO-LAST} => all keys/columns

  :FROM-FIRST and :TO-LAST are special keywords introduced in order to disambiguate the reverse order

  NOTE: range of keys shouldn't be used if Cassandra is configured with the default RandomPartitioner.
  They become useful only with the OrderPreservingPartitioner.
  
  Examples with Standard column families:
  ------------------------------------------------------
  =>(select \"Stars\" \"Rigel\" :distance) ; one row, one column
  772.51
  =>(select \"Stars\" \"Rigel\" {}) ; one row, all columns
  {:radius 78, :mass 17, :distance 772.51, :constellation \"Orion\"}
  =>(select \"Stars\" \"Rigel\" [:distance :mass]) ; one row, list of columns
  {:mass 17, :distance 772.51}
  =>(select \"Stars\" \"Rigel\" {:constellation :mass}) ; one row, range of columns
  {:mass 17, :distance 772.51, :constellation \"Orion\"}
  =>(select \"Stars\" {} {} 3 1) ; all keys and columns with limits of 3 rows and 1 col
  {\"Rigel\" {:constellation \"Orion\"}, \"Mu Cephei\" {:constellation \"Cepheus\"}, \"Aldebaran\" {:constellation \"Taurus\"}}
  =>(select \"Stars\" [\"Rigel\" \"Aldebaran\"] {}) ; list of rows, all columns
  {\"Rigel\" {:radius 78, :mass 17, :distance 772.51, :constellation \"Orion\"}, \"Aldebaran\" {:distance 65, :constellation \"Taurus\"}}
  =>(select \"Stars\" [\"Rigel\" \"Aldebaran\"] [:distance :mass]) ; list of rows, list of columns
  {\"Rigel\" {:mass 17, :distance 772.51}, \"Aldebaran\" {:distance 65}}

  Examples with Super column families:
  ------------------------------------------------------
  => (select \"Moons\" \"Saturn\" \"Titan\" :radius) ; one key, one super, one col
  2576
  => (select \"Moons\" \"Saturn\" \"Titan\" [:radius :temperature]) ; one key, one super, list of cols
  {:temperature 93.7, :radius 2576}
  => (select \"Moons\" \"Saturn\" \"Tethys\" {}) ; one key, one super, all cols
  {:temperature 86, :semi-major-axis 294619, :radius 533}
  => (select \"Moons\" \"Saturn\" {\"Abc\" \"Titan\"} {} 1) ; one key, range of super, all cols with limit of 1 super
  {\"Tethys\" {:temperature 86, :semi-major-axis 294619, :radius 533}}
  => (select \"Moons\" [\"Earth\" \"Saturn\"] \"Tethys\" {:aaa :sss}) ; list of keys, one super, range of cols with limit of 1 col 
  {\"Saturn\" {\"Tethys\" {:semi-major-axis 294619, :radius 533}}, \"Earth\" {\"Tethys\" {}}}

  All columns {} must be selected when using a list or a range of super columns.

  Where clauses and typed columns
  ------------------------------------------------------
  If your column is properly typed (see about typed columns on github). You can execute select statements with where clauses.
  If you just want the count of records found, you can use :COUNT instead of specifying columns.

  => (select \"Stars\" [:distance :mass] :WHERE= {:constellation \"Orion\"} :WHERE> {:distance 700})
  {\"Rigel\" {:mass 17, :distance 772}}
  => (select \"Stars\" :COUNT :WHERE= {:constellation \"Orion\"} :WHERE< {:distance 700})
  1

  Additional Notes
  ------------------------------------------------------
  Note about performance: setting a very high limit on number of rows seems to have a big performance 
  impact on Cassandra. Therefore, when not specified, the default limit of rows is 1 million. The 
  default limit of cols is Integer/MAX_VALUE.

  More examples are available in client_test.clj"
  {:arglists '([cf ks super* cols max-rows* max-super* max-cols*])}
  [& args]
  (let [res (apply dispatch-select args)]
    (if (map? res)
      (apply demap res args)
      res)))

(defn delete!
  "Delete data from Cassandra, for both standard and super column families.
  The following formats are supported:

  Standard column families:
  ------------------------------------------------------
  (delete! \"Stars\" \"Aldebaran\") ; delete 1 key
  (delete! \"Stars\" [\"Aldebaran\" \"Rigel\"]) ; delete many keys
  (delete! \"Stars\" \"Aldebaran\" :constellation) ; delete 1 column
  (delete! \"Stars\" \"Aldebaran\" [:constellation :distance]) ; delete many columns under 1 key
  (delete! \"Stars\" {\"Aldebaran\" [:constellation :distance]
                        \"Rigel\" [:constellation :distance :mass]}) ; delete many columns under many keys 
  (delete! {\"Stars\" [\"Aldebaran\" \"Rigel\"]
            \"Planets\" [\"Mars\" \"Jupiter\"]}) ; delete many keys under many CF
  (delete! {\"Stars\" \"Aldebaran\"
            \"Planets\" \"Mars\"}) ; delete 1 key under many CF
  (delete! {\"Stars\" \"Aldebaran\"
            \"Planets\" [\"Mars\" \"Jupiter\"]}) ; delete 1 key or many keys under many CF
  (delete! {\"Stars\" {\"Aldebaran\" [:constellation :distance]
                         \"Rigel\" [:constellation :distance :mass]}
            \"Planets\" {\"Mars\" :mass
                         \"Jupiter\" [:mass :radius]}}) ; delete many columns under many keys under many CF

  Super column families:
  ------------------------------------------------------
  (delete! \"Moons\" \"Saturn\") ; delete 1 key
  (delete! \"Moons\" [\"Saturn\" \"Earth\"]) ; delete many keys
  (delete! \"Moons\" \"Saturn\" \"Titan\" :radius) ; delete 1 column
  (delete! \"Moons\" \"Saturn\" \"Titan\" [:radius :temperature]) ; delete many columns
  (delete! \"Moons\" \"Saturn\" {\"Titan\" [:radius :temperature]
                                \"Tethys\" [:radius :temperature :semi-major-axis]) ; delete many columns under many super columns
  (delete! \"Moons\" {\"Saturn\" {\"Titan\" [:radius :temperature]
                                \"Tethys\" [:radius :temperature :semi-major-axis]} 
                      \"Earth\" {\"Moon\" :radius}}) ; delete many columns under many super columns under many keys
  (delete! {\"Moons\" {\"Saturn\" {\"Titan\" [:radius :temperature]
                                \"Tethys\" [:radius :temperature :semi-major-axis]} 
                      \"Earth\" {\"Moon\" :radius}} 
          \"Comets\" {\"Sun\" {\"Halley\" :mass}}}) ; delete many columns under many super columns under many keys
  (delete! {\"Moons\" [\"Saturn\" \"Earth\"]
          \"Comets\" {\"Sun\" {\"Halley\" :mass}}}) ; delete a mix of whole keys and specific columns

  NOTE: it is not possible to delete all columns for a given super column."
  ([tree]
    (internal-delete! tree))
  ([arg & args]
    (delete! (apply enmap arg args))))
