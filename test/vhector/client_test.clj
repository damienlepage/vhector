(ns vhector.client-test
  (:use [vhector.client :reload-all true]
        [clojure.test]))

(connect! "Test Cluster" "localhost" 9160 "Keyspace1")

(deftest insert-select-delete-simple
  (testing 
    "Insert a column, select it and delete it"
    (let [cf "Stars", k "Mu Cephei", col :mass, value 15.0]
      (insert! cf k col value)
      (is (= value (select cf k col)))
      (delete! cf k col)
      (is (nil? (select cf k col))))))

(deftest insert-select-delete-standard
  (testing 
    "Different kinds of insert!"
           
    (insert! {"Stars" {"Aldebaran" {:constellation "Taurus", :distance 65}
                           "Rigel" {:constellation "Orion", :distance 772, :mass 17.0, :radius 78.0}
                           "Mu Cephei" {:constellation "Cepheus", :radius 1650.0}}
              "Planets" {"Mars" {:mass 0.107}
                           "Jupiter" {:mass 317.8, :radius 10.517}}})
    (insert! "Stars" "Betelgeuse" {:constellation "Orion", :distance 643}))

  (testing 
    "Count using where clause"
    (is (= 2
          (select "Stars" :COUNT :WHERE= {:constellation "Orion"})))
    (is (= 1
          (select "Stars" :COUNT :WHERE= {:constellation "Orion"} :WHERE< {:distance 700}))))
  
  (testing 
    "Select using where clause"
    (is (= {"Rigel" {:distance 772 :mass 17.0}}
          (select "Stars" [:distance :mass] :WHERE= {:constellation "Orion"} :WHERE> {:distance 700})))
    (is (= {"Rigel" {:constellation "Orion", :distance 772, :mass 17.0, :radius 78.0}
            "Betelgeuse" {:constellation "Orion", :distance 643}}
          (select "Stars" {} :WHERE= {:constellation "Orion"} :WHERE< {:distance 800}))))
  
  (testing 
    "Select 1 key, 1 col"    
    (is (= 643
          (select "Stars" "Betelgeuse" :distance))))
    
  (testing 
    "Select 1 key, list of cols"
    (is (=
          {:mass 17.0, :distance 772}
          (select "Stars" "Rigel" [:distance :mass]))))

  (testing 
    "Select 1 key, all cols"
    (is (=
          {:radius 78.0, :mass 17.0, :distance 772, :constellation "Orion"}
          (select "Stars" "Rigel" {}))))

  (testing 
    "Select 1 key, all cols with limit"
    (is (=
          {:distance 772, :constellation "Orion"}
          (select "Stars" "Rigel" {} 2))))
  
  (testing 
    "Select 1 key, range of cols"
    (is (= 
          {:constellation "Orion", :distance 772, :mass 17.0}
          (select "Stars" "Rigel" {:constellation :mass}))))

  (testing 
    "Select 1 key, range of cols with limit"
    (is (=
          {:constellation "Orion"}
          (select "Stars" "Rigel" {:aaa :ppp} 1))))
    
  (testing 
    "Select 1 key, range of cols without begin limit"
    (is (= 
          {:constellation "Orion", :distance 772, :mass 17.0}
          (select "Stars" "Rigel" {nil :mass}))))

  (testing
    "Select 1 key, range of cols without end limit"
    (is (= 
          {:mass 17.0, :radius 78.0}
          (select "Stars" "Rigel" {:mass nil}))))
    
  (testing 
    "Select list of keys, all cols"
    (is (= 
          {"Mu Cephei" {:radius 1650.0, :constellation "Cepheus"}
           "Aldebaran" {:distance 65, :constellation "Taurus"}}
          (select "Stars" ["Aldebaran" "Mu Cephei"] {}))))

  (testing 
    "Select list of keys, all cols with limit"
    (is (= 
          {"Mu Cephei" {:constellation "Cepheus"}
           "Aldebaran" {:constellation "Taurus"}}
          (select "Stars" ["Aldebaran" "Mu Cephei"] {} 1))))

  (testing 
    "Select list of keys, list of cols"
    (is (= 
          {"Mu Cephei" {:radius 1650.0, :constellation "Cepheus"}
           "Aldebaran" {:constellation "Taurus"}}
          (select "Stars" ["Aldebaran" "Mu Cephei"] [:constellation :radius]))))

  (testing 
    "Select list of keys, range of cols"
    (is (= 
          {"Mu Cephei" {:constellation "Cepheus"}
           "Aldebaran" {:distance 65, :constellation "Taurus"}}
          (select "Stars" ["Aldebaran" "Mu Cephei"] {:constellation :distance}))))

  (testing 
    "Select list of keys, range of cols in reverse order"
    (is (= 
          {"Mu Cephei" {:constellation "Cepheus"}
           "Aldebaran" {:constellation "Taurus", :distance 65}}
          (select "Stars" ["Aldebaran" "Mu Cephei"] {:distance :constellation}))))

  (testing 
    "Select list of keys, all cols in reverse order"
    (is (= 
          {"Mu Cephei" {:constellation "Cepheus", :radius 1650.0}
           "Aldebaran" {:constellation "Taurus", :distance 65}}
          (select "Stars" ["Aldebaran" "Mu Cephei"] {:TO-LAST :FROM-FIRST}))))

  (testing 
    "Select list of keys, range of cols with limit"
    (is (= 
          {"Mu Cephei" {:constellation "Cepheus"}
           "Aldebaran" {:constellation "Taurus"}}
          (select "Stars" ["Aldebaran" "Mu Cephei"] {:constellation :distance} 1))))

  (testing 
    "Select range of keys, all cols"
    (is (= 
          {"Rigel" {:radius 78.0, :mass 17.0, :distance 772, :constellation "Orion"}
           "Mu Cephei" {:radius 1650.0, :constellation "Cepheus"}
           "Aldebaran" {:distance 65, :constellation "Taurus"}}
          (select "Stars" {"Aldebaran" "Rigel"} {}))))

  (testing 
    "Select range of keys, all cols with limits"
    (is (= 
          {"Mu Cephei" {:constellation "Cepheus"}
           "Aldebaran" {:constellation "Taurus"}}
          (select "Stars" {"Aldebaran" "Rigel"} {} 2 1))))

  (testing 
    "Select range of keys, one col"
    (is (=
          {"Rigel" {:radius 78.0}
           "Mu Cephei" {:radius 1650.0}
           "Aldebaran" {}}
          (select "Stars" {"Aldebaran" "Rigel"} :radius))))

  
  (testing 
    "Select range of keys, list of cols"
    (is (=
          {"Rigel" {:radius 78.0, :distance 772}
           "Mu Cephei" {:radius 1650.0}
           "Aldebaran" {:distance 65}}
          (select "Stars" {"Aldebaran" "Rigel"} [:radius :distance]))))

  (testing 
    "Select range of keys, range of cols"
    (is (= 
          {"Rigel" {:distance 772, :constellation "Orion"}
           "Mu Cephei" {:constellation "Cepheus"}
           "Aldebaran" {:distance 65, :constellation "Taurus"}}
          (select "Stars" {"Aldebaran" "Rigel"} {:aaa :distance}))))
  
  (testing 
    "Delete list of col"
    (delete! "Stars" "Rigel" [:constellation :distance])
    (is (= {:mass 17.0, :radius 78.0}
          (select "Stars" "Rigel" {}))))

  (testing 
    "Delete complete key"
    (delete! "Stars" "Rigel")
    (is (nil? (select "Stars" "Rigel" {}))))
  
  (testing 
    "Mix of many delete operations"
    (are [query] (not= nil query) 
         (select "Stars" "Mu Cephei" {})
         (select "Stars" "Betelgeuse" {})
         (select "Planets" "Mars" {})
         (select "Planets" "Jupiter" {}))
    (delete! {"Stars" ["Mu Cephei" "Betelgeuse"]
              "Planets" {"Mars" :mass
                           "Jupiter" [:mass :radius]}})
    (are [query] (nil? query)
         (select "Stars" "Mu Cephei" {})
         (select "Stars" "Betelgeuse" {})
         (select "Planets" "Mars" {})
         (select "Planets" "Jupiter" {}))))

(deftest insert-select-delete-super
    (testing 
      "Different kinds of insert!"

      (insert! "Moons" "Saturn" "Titan" :radius 2576.0)
      (insert! "Moons" "Saturn" "Titan" {:radius 2576.0, :temperature 93.7})
      (insert! "Moons" "Saturn" {"Titan" {:radius 2576.0, :temperature 93.7}
                                  "Tethys" {:radius 533.0, :temperature 86.0, :semi-major-axis 294619}})
      (insert! "Moons" {"Saturn" {"Titan" {:radius 2576.0, :temperature 93.7}
                                   "Tethys" {:radius 533.0, :temperature 86.0, :semi-major-axis 294619}}
                         "Earth" {"Moon" {:radius 1737.10}}})
      (insert! {"Moons" {"Saturn" {"Titan" {:radius 2576.0, :temperature 93.7}
                                    "Tethys" {:radius 533.0, :temperature 86.0, :semi-major-axis 294619}}
                          "Earth" {"Moon" {:radius 1737.10}}}
                "Comets" {"Sun" {"Halley" {:mass 2.2e14}}}}))
    
    (testing
      "Select 1 key, 1 super, 1 col"    
      (is (= 2576.0
             (select "Moons" "Saturn" "Titan" :radius))))
    
    (testing 
      "Select 1 key, 1 super, list of cols"
      (is (= {:radius 2576.0, :temperature 93.7}
             (select "Moons" "Saturn" "Titan" [:radius :temperature]))))
    
    (testing 
      "Select 1 key, 1 super, all cols"
      (is (= {:radius 533.0, :temperature 86.0, :semi-major-axis 294619}
             (select "Moons" "Saturn" "Tethys" {}))))

    (testing 
      "Select 1 key, 1 super, range of cols"
      (is (= {:temperature 86.0, :semi-major-axis 294619}
             (select "Moons" "Saturn" "Tethys" {:semi-major-axis :temperature}))))
    
    (testing 
      "Select 1 key, 1 super, all cols with limit"
      (is (= {:radius 533.0}
             (select "Moons" "Saturn" "Tethys" {} 1))))
    
    (testing 
      "Select 1 key, list of super"
      (is (= {"Titan" {:radius 2576.0, :temperature 93.7}
              "Tethys" {:radius 533.0, :temperature 86.0, :semi-major-axis 294619}}
             (select "Moons" "Saturn" ["Tethys" "Titan"] {}))))
    
    (testing 
      "Select 1 key, range of super with limit"
      (is (= {"Tethys" {:radius 533.0, :temperature 86.0, :semi-major-axis 294619}}
             (select "Moons" "Saturn" {"Abc" "Titan"} {} 1))))

    (testing 
      "Select 1 key, range of super, one col => unsupported"
      (is (= "All columns {} must be selected when using a list or a range of super columns."
             (try
               (select "Moons" "Saturn" {"Tethys" "Titan"} :temperature)
               (catch Exception e (.getMessage e))))))

    (testing 
      "Select list of keys, 1 super, 1 col"
      (is (= {"Saturn" {"Moon" {}}, "Earth" {"Moon" {:radius 1737.1}}}
             (select "Moons" ["Earth" "Saturn"] "Moon" :radius))))
    
    (testing 
      "Select list of keys, 1 super, list of cols"
      (is (= {"Saturn" {"Tethys" {:radius 533.0, :temperature 86.0}}, "Earth" {"Tethys" {}}}
             (select "Moons" ["Earth" "Saturn"] "Tethys" [:radius :temperature]))))

    (testing 
      "Select list of keys, 1 super, range of cols"
      (is (= {"Saturn" {"Tethys" {:radius 533.0, :semi-major-axis 294619}}, "Earth" {"Tethys" {}}}
             (select "Moons" ["Earth" "Saturn"] "Tethys" {:aaa :sss}))))

    (testing 
      "Select list of keys, 1 super, range of cols in reverse order"
      (is (= {"Saturn" {"Tethys" {:semi-major-axis 294619, :radius 533.0}}, "Earth" {"Tethys" {}}}
             (select "Moons" ["Earth" "Saturn"] "Tethys" {:sss :aaa}))))

    (testing 
      "Select list of keys, list of super"
      (is (= {"Saturn" {"Tethys" {:radius 533.0, :temperature 86.0, :semi-major-axis 294619}}
              "Earth" {"Moon" {:radius 1737.10}}}
             (select "Moons" ["Earth" "Saturn"] ["Moon" "Tethys"] {}))))

    (testing 
      "Select list of keys, range of super"
      (is (= {"Saturn" {"Titan" {:temperature 93.7, :radius 2576.0}
                        "Tethys" {:radius 533.0, :temperature 86.0, :semi-major-axis 294619}}
              "Earth" {}}
             (select "Moons" ["Earth" "Saturn"] {"Tethys" "Titan"} {}))))

    (testing 
      "Select range of keys, all super, all cols with limits of 2 rows and 1 super"
      (is (= {"Saturn" {"Tethys" {:radius 533.0, :temperature 86.0, :semi-major-axis 294619}}
              "Earth" {"Moon" {:radius 1737.10}}}
             (select "Moons" {"Earth" "Saturn"} {} {} 2 1))))

    (testing 
      "Select range of keys, one super, list of cols"
      (is (= {"Saturn" {"Tethys" {:temperature 86.0, :radius 533.0}}
              "Earth" {"Tethys" {}}}
             (select "Moons" {"Earth" "Saturn"} "Tethys" [:radius :temperature]))))
    
    (testing 
      "Delete one col"
      (is (= 2576.0 (select "Moons" "Saturn" "Titan" :radius)))
      (delete! "Moons" "Saturn" "Titan" :radius)
      (is (nil? (select "Moons" "Saturn" "Titan" :radius))))

    (testing 
      "Delete many col"
      (is (not= {:radius 533.0} (select "Moons" "Saturn" "Tethys" {})))
      (delete! "Moons" "Saturn" "Tethys" [:semi-major-axis :temperature])
      (is (= {:radius 533.0} (select "Moons" "Saturn" "Tethys" {}))))

    (testing 
      "Delete complete key"
      (is (not= nil (select "Moons" "Earth" "Moon" {})))
      (delete! "Moons" "Earth")
      (is (nil? (select "Moons" "Earth" "Moon" {}))))
        
    (testing 	
      "Mix of many delete operations"
      (are [query] (not= nil query) 
           (select "Moons" "Saturn" "Titan" {})
           (select "Comets" "Sun" "Halley" {}))
      (delete! {"Moons" ["Saturn" "Earth"]
                "Comets" {"Sun" {"Halley" :mass}}})
      (are [query] (nil? query) 
           (select "Moons" "Saturn" "Titan" {})
           (select "Comets" "Sun" "Halley" {}))))

(deftest with-cass-test
  (testing 
    "Test change connection"
    (with-cass "Test Cluster" "localhost" 9160 "Keyspace1"
      (let [cf "Stars", k "Mu Cephei", col :mass, value 15.0]
        (insert! cf k col value)
        (is (= value (select cf k col)))
        (delete! cf k col)))))
