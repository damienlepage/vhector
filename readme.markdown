Vhector
=======

Vhector is a Clojure wrapper for [Hector](https://github.com/rantav/hector), 
high level client for [Apache Cassandra](http://cassandra.apache.org/).

Releases
------------

0.3.0 : Add support for typed columns (Long and Double) and where clauses (see section Typed Columns below)  
0.2.0 : Upgrade to Clojure 1.3, Hector 1.0-2 tested with Cassandra 1.0.7  
0.1.0 : Initial version with Clojure 1.2, Hector 0.7, tested with Cassandra 0.7

Installation
------------

You can get the binary and its dependencies from Clojars, see [http://clojars.org/vhector](http://clojars.org/vhector)

To use Vhector with Leiningen, just add [vhector "0.2.0"] to your project.clj.

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
    
    (insert! "Stars" "Aldebaran" :constellation "Taurus")
    ;=> #<MutationResultImpl MutationResult took (236us) for query (n/a) on host: localhost(127.0.0.1):9160>
    
    (select "Stars" "Aldebaran" :constellation)
    ;=> "Taurus"

    (delete! "Stars" "Aldebaran" :constellation)
    ;=> #<MutationResultImpl MutationResult took (4us) for query (n/a) on host: localhost(127.0.0.1):9160>

Typed Columns
-------------

In order to benefit of Clojure dynamic typing, by default Vhector inserts the string representation of each value.
Therefore, "Aldebaran" is inserted with its quotes so it can be retrieved as a string dynamically,
while :constellation can be retrieved as a keyword.

However, when selecting by range or using where clauses with operators <,>,<=,>= on numeric values the column in Cassandra must be "typed" with the proper validation class. The storage is still an array of bytes but Cassandra becomes able to sort the values based on the proper type.

Storing dynamically with the proper type was not an issue for Vhector. However infering the proper type from the DB was a challenge since loading into a Long or a String a bytes array representing a Double could work with no error and simply mess up the data. One option would have been to specify the data type with meta-data on the API but it would have make it more complicated for the user.

Therefore, the solution provided by Vhector by default, is to automatically suffix the column name with the proper type (?double when data is of type Double or Float, ?long when data is a Long, Integer or Short). On select, Vhector will fetch all the possible columns and keep only one result.

Example:

    (insert! "Stars" "Aldebaran" :distance 65)
    ; will result into a column ":distance?long" persisted in Cassandra.  

Then

    (select "Stars" "Aldebaran" :distance)
    ; will try fetching results from ":distance", ":distance?long" and "distance?double" columns, 
    ; then keep only one under the name :distance. When fetching all columns (or a range) all suffixes are also removed.


If you don't want this behavior, you can specify an extra argument to the connect function:

    (connect! "Test Cluster" "localhost" 9160 "Keyspace1" false)

You can also temporary enable/disable this behavior with the macro:

    (with-typed-columns false
       (insert! "Stars" "Aldebaran" :distance 65))
    ; will result in the string "65" being stored in column ":distance"

***Important!*** For ranges and where clauses to work properly, the client is responsible for always providing the same type of data on any given column. Be especially careful not to mix double with Clojure's ratio (saved as string) or long. 


Inserting or deleting many values at once
-----------------------------------------

The insert! and delete! functions support nested maps as arguments to target many columns at once. 

    (insert! "Stars" {
        "Aldebaran" {:constellation "Taurus", :distance 65}
        "Rigel" {:constellation "Orion", :distance 772.51, :mass 17}})
    ;=> #<MutationResultImpl MutationResult took (3us) for query (n/a) on host: localhost(127.0.0.1):9160>

    (delete! "Stars" {
        "Aldebaran" [:constellation :distance]
        "Rigel" [:constellation :distance :mass]})
    ;=> #<MutationResultImpl MutationResult took (3us) for query (n/a) on host: localhost(127.0.0.1):9160>

You can also delete a entire key like this:

    (delete! "Stars" "Rigel")
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

    (select "Stars" "Rigel" [:distance :mass]) ; one row, list of columns
    ;=> {:mass 17, :distance 772.51}

    (select "Stars" ["Rigel" "Aldebaran"] {}) ; list of rows, all columns
    ;=> {"Rigel" {:radius 78, :mass 17, :distance 772.51, :constellation "Orion"}, "Aldebaran" {:distance 65, :constellation "Taurus"}}

### Limits

When using ranges, you can set-up the limit of keys or columns returned.

    (select "Stars" {} {} 3 1) ; all keys and columns with limits of 3 rows and 1 col
    ;=> {"Rigel" {:constellation "Orion"}, "Mu Cephei" {:constellation "Cepheus"}, "Aldebaran" {:constellation "Taurus"}}


Selecting with where clauses
----------------------------

Works only on Standard column families. This is a limitation of Cassandra.

It is possible to specify where clauses instead of keys in the select function.  
When using where clauses, it is also possible to count the results rather than returning them.  

Under the hood, these functionalities are implemented using the Cassandra Query Language (CQL).

    (select :COUNT "Stars" :WHERE= {:constellation "Orion"} :WHERE< {:distance 700})
    ;=> 1
    
    => (select "Stars" [:distance :mass] :WHERE= {:constellation "Orion"} :WHERE> {:mass 10.0, :distance 500} :WHERE<= {:radius 78.0})
    ;=> {"Rigel" {:mass 17.0, :distance 772}}

***Troubleshooting:***  
If you get the following kind of error message it means you didn't set the proper validator class on your column in Cassandra.  
<InvalidRequestException InvalidRequestException(why:cannot parse '78.0' as hex bytes)>


Working with super columns
--------------------------

Vhector doesn't really make a difference between a super column and a standard column. 
It simply uses an extra argument or extra nested map.

    (insert! "Moons" {
        "Saturn" {
            "Titan" {:radius 2576, :temperature 93.7}
            "Tethys" {:radius 533, :temperature 86, :semi-major-axis 294619}}
        "Earth" {
            "Moon" {:radius 1737.10}}})
    ;=> #<MutationResultImpl MutationResult took (4us) for query (n/a) on host: localhost(127.0.0.1):9160>

    (select "Moons" "Saturn" "Titan" [:radius :temperature])
    ;=> {:temperature 93.7, :radius 2576}

Connecting to multiple keyspaces or clusters
--------------------------------------------

To override the connection set-up with connect!, you can use the macro with-cass like this:

    (with-cass "Test Cluster" "localhost" 9160 "Keyspace2"
        (insert! "Stars" "Aldebaran" :constellation "Taurus"))

More
----

The Clojure docs of each function contains more details and examples than described here.  
You can also check the test suite in client_test.clj for even more examples.
