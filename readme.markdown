Vhector
=======

Vhector is a Clojure wrapper for [Hector](https://github.com/rantav/hector), 
high level client for [Apache Cassandra](http://cassandra.apache.org/).

Installation
------------

You can get the binary and its dependencies from Clojars, see [http://clojars.org/vhector](http://clojars.org/vhector)

To use Vhector with Leiningen, just add [vhector "0.1.0-SNAPSHOT"] to your project.clj.

Overview
--------

Vhector offers almost the full power of hector with a very simple API of 4 idiomatic functions:

* connect!
* insert! 
* select 
* delete!

Getting started
---------------

Here is a basic example which connects to a Cassandra database, insert a value, retrieve it and delete it.

    (use 'vhector.client)
    ;=> nil
    
    (connect! "Test Cluster" "localhost" 9160 "Keyspace1")
    ;=> #'vhector.internal.hector/*keyspace*
    
    (insert! "Standard1" "Aldebaran" :constellation "Taurus")
    ;=> #<MutationResultImpl MutationResult took (236us) for query (n/a) on host: localhost(127.0.0.1):9160>
    
    (select "Standard1" "Aldebaran" :constellation)
    ;=> "Taurus"

    (delete! "Standard1" "Aldebaran" :constellation)
    ;=> #<MutationResultImpl MutationResult took (4us) for query (n/a) on host: localhost(127.0.0.1):9160>


Inserting or deleting many values at once
-----------------------------------------

The insert! and delete! functions support nested maps as arguments to target many columns at once. 

    (insert! "Standard1" {
        "Aldebaran" {:constellation "Taurus", :distance 65}
        "Rigel" {:constellation "Orion", :distance 772.51, :mass 17}})
    ;=> #<MutationResultImpl MutationResult took (3us) for query (n/a) on host: localhost(127.0.0.1):9160>

    (delete! "Standard1" {
        "Aldebaran" [:constellation :distance]
        "Rigel" [:constellation :distance :mass]})
    ;=> #<MutationResultImpl MutationResult took (3us) for query (n/a) on host: localhost(127.0.0.1):9160>

You can also delete a entire key like this:

    (delete! "Standard1" "Rigel")
    ;=> #<MutationResultImpl MutationResult took (2us) for query (n/a) on host: localhost(127.0.0.1):9160>

Selecting many records at once
------------------------------

The arguments of select! can use different forms to target many records at once.

* string: one key or one column
* vector: list of keys/columns
* map: range of keys/columns with support for unlimited end. 
  {:start :stop} => all keys/columns between :start and :stop (inclusive)
  {:start nil} => all keys/columns from :start to the end
  {nil :stop} => all keys/columns from the beginning to :stop
  {} or {nil nil} => all keys/columns

The returned value is adjusted to the depth of variable arguments.

### Examples

    (select "Standard1" "Rigel" [:distance :mass]) ; one row, list of columns
    ;=> {:mass 17, :distance 772.51}

    (select "Standard1" ["Rigel" "Aldebaran"] {}) ; list of rows, all columns
    ;=> {"Rigel" {:radius 78, :mass 17, :distance 772.51, :constellation "Orion"}, "Aldebaran" {:distance 65, :constellation "Taurus"}}

### Limits

When using ranges, you can set-up the limit of keys or columns returned.

    (select "Standard1" {} {} 3 1) ; all keys and columns with limits of 3 rows and 1 col
    ;=> {"Rigel" {:constellation "Orion"}, "Mu Cephei" {:constellation "Cepheus"}, "Aldebaran" {:constellation "Taurus"}}

Working with super columns
--------------------------

Vhector doesn't really make a difference between a super column and a standard column. 
It simply uses an extra argument or extra nested map.

    (insert! "Super1" {
        "Saturn" {
            "Titan" {:radius 2576, :temperature 93.7}
            "Tethys" {:radius 533, :temperature 86, :semi-major-axis 294619}}
        "Earth" {
            "Moon" {:radius 1737.10}}})
    ;=> #<MutationResultImpl MutationResult took (4us) for query (n/a) on host: localhost(127.0.0.1):9160>

    (select "Super1" "Saturn" "Titan" [:radius :temperature])
    ;=> {:temperature 93.7, :radius 2576}

Connecting to multiple keyspaces or clusters
--------------------------------------------

To override the connection set-up with connect!, you can use the macro with-cass like this:

    (with-cass "Test Cluster" "localhost" 9160 "Keyspace2"
        (insert! "Standard1" "Aldebaran" :constellation "Taurus"))
        
Note
----

In order to benefit of Clojure dynamic typing, Vhector inserts the string representation of each value.
Therefore, "Aldebaran" is inserted with its quotes so it can be retrieved as a string dynamically,
while :constellation can be retrieved as a keyword.

More
----

The Clojure docs of each function contains more details and examples than described here. 
