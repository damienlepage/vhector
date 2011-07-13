(ns vhector.client-test
  (:use [vhector.client :reload-all true]
        [clojure.test]))

(connect! "Test Cluster" "localhost" 9160 "Keyspace1")

(deftest insert-select-delete-simple
  (testing 
    "Insert a column, select it and delete it"
    (let [cf "Standard1", k "Mu Cephei", col :mass, value 15]
      (insert! cf k col value)
      (is (= value (select cf k col)))
      (delete! cf k col)
      (is (nil? (select cf k col))))))

(deftest insert-select-delete-standard
  (testing 
    "Different kinds of insert!"
           
    (insert! {"Standard1" {"Aldebaran" {:constellation "Taurus", :distance 65}
                           "Rigel" {:constellation "Orion", :distance 772.51, :mass 17, :radius 78}
                           "Mu Cephei" {:constellation "Cepheus", :radius 1650}}
              "Standard2" {"Mars" {:mass 0.107}
                           "Jupiter" {:mass 317.8, :radius 10.517}}})
    (insert! "Standard1" "Betelgeuse" {:constellation "Orion", :distance 643}))
  
  (testing 
    "Select 1 key, 1 col"    
    (is (= 643
          (select "Standard1" "Betelgeuse" :distance))))
    
  (testing 
    "Select 1 key, list of cols"
    (is (=
          {:mass 17, :distance 772.51}
          (select "Standard1" "Rigel" [:distance :mass]))))

  (testing 
    "Select 1 key, all cols"
    (is (=
          {:radius 78, :mass 17, :distance 772.51, :constellation "Orion"}
          (select "Standard1" "Rigel" {}))))

  (testing 
    "Select 1 key, all cols with limit"
    (is (=
          {:distance 772.51, :constellation "Orion"}
          (select "Standard1" "Rigel" {} 2))))
  
  (testing 
    "Select 1 key, range of cols"
    (is (= 
          {:constellation "Orion", :distance 772.51, :mass 17}
          (select "Standard1" "Rigel" {:constellation :mass}))))

  (testing 
    "Select 1 key, range of cols with limit"
    (is (=
          {:constellation "Orion"}
          (select "Standard1" "Rigel" {:aaa :ppp} 1))))
    
  (testing 
    "Select 1 key, range of cols without begin limit"
    (is (= 
          {:constellation "Orion", :distance 772.51, :mass 17}
          (select "Standard1" "Rigel" {nil :mass}))))

  (testing
    "Select 1 key, range of cols without end limit"
    (is (= 
          {:mass 17, :radius 78}
          (select "Standard1" "Rigel" {:mass nil}))))
    
  (testing 
    "Select list of keys, all cols"
    (is (= 
          {"Mu Cephei" {:radius 1650, :constellation "Cepheus"}
           "Aldebaran" {:distance 65, :constellation "Taurus"}}
          (select "Standard1" ["Aldebaran" "Mu Cephei"] {}))))

  (testing 
    "Select list of keys, all cols with limit"
    (is (= 
          {"Mu Cephei" {:constellation "Cepheus"}
           "Aldebaran" {:constellation "Taurus"}}
          (select "Standard1" ["Aldebaran" "Mu Cephei"] {} 1))))

  (testing 
    "Select list of keys, list of cols"
    (is (= 
          {"Mu Cephei" {:radius 1650, :constellation "Cepheus"}
           "Aldebaran" {:constellation "Taurus"}}
          (select "Standard1" ["Aldebaran" "Mu Cephei"] [:constellation :radius]))))

  (testing 
    "Select list of keys, range of cols"
    (is (= 
          {"Mu Cephei" {:constellation "Cepheus"}
           "Aldebaran" {:distance 65, :constellation "Taurus"}}
          (select "Standard1" ["Aldebaran" "Mu Cephei"] {:constellation :distance}))))

  (testing 
    "Select list of keys, range of cols in reverse order"
    (is (= 
          {"Mu Cephei" {:constellation "Cepheus"}
           "Aldebaran" {:constellation "Taurus", :distance 65}}
          (select "Standard1" ["Aldebaran" "Mu Cephei"] {:distance :constellation}))))

  (testing 
    "Select list of keys, all cols in reverse order"
    (is (= 
          {"Mu Cephei" {:constellation "Cepheus", :radius 1650}
           "Aldebaran" {:constellation "Taurus", :distance 65}}
          (select "Standard1" ["Aldebaran" "Mu Cephei"] {:TO-LAST :FROM-FIRST}))))

  (testing 
    "Select list of keys, range of cols with limit"
    (is (= 
          {"Mu Cephei" {:constellation "Cepheus"}
           "Aldebaran" {:constellation "Taurus"}}
          (select "Standard1" ["Aldebaran" "Mu Cephei"] {:constellation :distance} 1))))

  (testing 
    "Select range of keys, all cols"
    (is (= 
          {"Rigel" {:radius 78, :mass 17, :distance 772.51, :constellation "Orion"}
           "Mu Cephei" {:radius 1650, :constellation "Cepheus"}
           "Aldebaran" {:distance 65, :constellation "Taurus"}}
          (select "Standard1" {"Aldebaran" "Rigel"} {}))))

  (testing 
    "Select range of keys, all cols with limits"
    (is (= 
          {"Mu Cephei" {:constellation "Cepheus"}
           "Aldebaran" {:constellation "Taurus"}}
          (select "Standard1" {"Aldebaran" "Rigel"} {} 2 1))))

  (testing 
    "Select range of keys, one col"
    (is (=
          {"Rigel" {:radius 78}
           "Mu Cephei" {:radius 1650}
           "Aldebaran" {}}
          (select "Standard1" {"Aldebaran" "Rigel"} :radius))))

  
  (testing 
    "Select range of keys, list of cols"
    (is (=
          {"Rigel" {:radius 78, :distance 772.51}
           "Mu Cephei" {:radius 1650}
           "Aldebaran" {:distance 65}}
          (select "Standard1" {"Aldebaran" "Rigel"} [:radius :distance]))))

  (testing 
    "Select range of keys, range of cols"
    (is (= 
          {"Rigel" {:distance 772.51, :constellation "Orion"}
           "Mu Cephei" {:constellation "Cepheus"}
           "Aldebaran" {:distance 65, :constellation "Taurus"}}
          (select "Standard1" {"Aldebaran" "Rigel"} {:aaa :distance}))))
  
  (testing 
    "Delete list of col"
    (delete! "Standard1" "Rigel" [:constellation :distance])
    (is (= {:mass 17, :radius 78}
          (select "Standard1" "Rigel" {}))))

  (testing 
    "Delete complete key"
    (delete! "Standard1" "Rigel")
    (is (nil? (select "Standard1" "Rigel" {}))))
  
  (testing 
    "Mix of many delete operations"
    (are [query] (not= nil query) 
         (select "Standard1" "Mu Cephei" {})
         (select "Standard1" "Betelgeuse" {})
         (select "Standard2" "Mars" {})
         (select "Standard2" "Jupiter" {}))
    (delete! {"Standard1" ["Mu Cephei" "Betelgeuse"]
              "Standard2" {"Mars" :mass
                           "Jupiter" [:mass :radius]}})
    (are [query] (nil? query)
         (select "Standard1" "Mu Cephei" {})
         (select "Standard1" "Betelgeuse" {})
         (select "Standard2" "Mars" {})
         (select "Standard2" "Jupiter" {}))))

(deftest insert-select-delete-super
    (testing 
      "Different kinds of insert!"

      (insert! "Super1" "Saturn" "Titan" :radius 2576)
      (insert! "Super1" "Saturn" "Titan" {:radius 2576, :temperature 93.7})
      (insert! "Super1" "Saturn" {"Titan" {:radius 2576, :temperature 93.7}
                                  "Tethys" {:radius 533, :temperature 86, :semi-major-axis 294619}})
      (insert! "Super1" {"Saturn" {"Titan" {:radius 2576, :temperature 93.7}
                                   "Tethys" {:radius 533, :temperature 86, :semi-major-axis 294619}}
                         "Earth" {"Moon" {:radius 1737.10}}})
      (insert! {"Super1" {"Saturn" {"Titan" {:radius 2576, :temperature 93.7}
                                    "Tethys" {:radius 533, :temperature 86, :semi-major-axis 294619}}
                          "Earth" {"Moon" {:radius 1737.10}}}
                "Super2" {"Sun" {"Halley" {:mass "2.2*1014"}}}}))
    
    (testing
      "Select 1 key, 1 super, 1 col"    
      (is (= 2576
             (select "Super1" "Saturn" "Titan" :radius))))
    
    (testing 
      "Select 1 key, 1 super, list of cols"
      (is (= {:radius 2576, :temperature 93.7}
             (select "Super1" "Saturn" "Titan" [:radius :temperature]))))
    
    (testing 
      "Select 1 key, 1 super, all cols"
      (is (= {:radius 533, :temperature 86, :semi-major-axis 294619}
             (select "Super1" "Saturn" "Tethys" {}))))

    (testing 
      "Select 1 key, 1 super, range of cols"
      (is (= {:temperature 86, :semi-major-axis 294619}
             (select "Super1" "Saturn" "Tethys" {:semi-major-axis :temperature}))))
    
    (testing 
      "Select 1 key, 1 super, all cols with limit"
      (is (= {:radius 533}
             (select "Super1" "Saturn" "Tethys" {} 1))))
    
    (testing 
      "Select 1 key, list of super"
      (is (= {"Titan" {:radius 2576, :temperature 93.7}
              "Tethys" {:radius 533, :temperature 86, :semi-major-axis 294619}}
             (select "Super1" "Saturn" ["Tethys" "Titan"] {}))))
    
    (testing 
      "Select 1 key, range of super with limit"
      (is (= {"Tethys" {:radius 533, :temperature 86, :semi-major-axis 294619}}
             (select "Super1" "Saturn" {"Abc" "Titan"} {} 1))))

    (testing 
      "Select 1 key, range of super, one col => unsupported"
      (is (= "All columns {} must be selected when using a list or a range of super columns."
             (try
               (select "Super1" "Saturn" {"Tethys" "Titan"} :temperature)
               (catch Exception e (.getMessage e))))))

    (testing 
      "Select list of keys, 1 super, 1 col"
      (is (= {"Saturn" {"Moon" {}}, "Earth" {"Moon" {:radius 1737.1}}}
             (select "Super1" ["Earth" "Saturn"] "Moon" :radius))))
    
    (testing 
      "Select list of keys, 1 super, list of cols"
      (is (= {"Saturn" {"Tethys" {:radius 533, :temperature 86}}, "Earth" {"Tethys" {}}}
             (select "Super1" ["Earth" "Saturn"] "Tethys" [:radius :temperature]))))

    (testing 
      "Select list of keys, 1 super, range of cols"
      (is (= {"Saturn" {"Tethys" {:radius 533, :semi-major-axis 294619}}, "Earth" {"Tethys" {}}}
             (select "Super1" ["Earth" "Saturn"] "Tethys" {:aaa :sss}))))

    (testing 
      "Select list of keys, 1 super, range of cols in reverse order"
      (is (= {"Saturn" {"Tethys" {:semi-major-axis 294619, :radius 533}}, "Earth" {"Tethys" {}}}
             (select "Super1" ["Earth" "Saturn"] "Tethys" {:sss :aaa}))))

    (testing 
      "Select list of keys, list of super"
      (is (= {"Saturn" {"Tethys" {:radius 533, :temperature 86, :semi-major-axis 294619}}
              "Earth" {"Moon" {:radius 1737.10}}}
             (select "Super1" ["Earth" "Saturn"] ["Moon" "Tethys"] {}))))

    (testing 
      "Select list of keys, range of super"
      (is (= {"Saturn" {"Titan" {:temperature 93.7, :radius 2576}
                        "Tethys" {:radius 533, :temperature 86, :semi-major-axis 294619}}
              "Earth" {}}
             (select "Super1" ["Earth" "Saturn"] {"Tethys" "Titan"} {}))))

    (testing 
      "Select range of keys, all super, all cols with limits of 2 rows and 1 super"
      (is (= {"Saturn" {"Tethys" {:radius 533, :temperature 86, :semi-major-axis 294619}}
              "Earth" {"Moon" {:radius 1737.10}}}
             (select "Super1" {"Earth" "Saturn"} {} {} 2 1))))

    (testing 
      "Select range of keys, one super, list of cols"
      (is (= {"Saturn" {"Tethys" {:temperature 86, :radius 533}}
              "Earth" {"Tethys" {}}}
             (select "Super1" {"Earth" "Saturn"} "Tethys" [:radius :temperature]))))
    
    (testing 
      "Delete one col"
      (is (= 2576 (select "Super1" "Saturn" "Titan" :radius)))
      (delete! "Super1" "Saturn" "Titan" :radius)
      (is (nil? (select "Super1" "Saturn" "Titan" :radius))))

    (testing 
      "Delete many col"
      (is (not= {:radius 533} (select "Super1" "Saturn" "Tethys" {})))
      (delete! "Super1" "Saturn" "Tethys" [:semi-major-axis :temperature])
      (is (= {:radius 533} (select "Super1" "Saturn" "Tethys" {}))))

    (testing 
      "Delete complete key"
      (is (not= nil (select "Super1" "Earth" "Moon" {})))
      (delete! "Super1" "Earth")
      (is (nil? (select "Super1" "Earth" "Moon" {}))))
        
    (testing 	
      "Mix of many delete operations"
      (are [query] (not= nil query) 
           (select "Super1" "Saturn" "Titan" {})
           (select "Super2" "Sun" "Halley" {}))
      (delete! {"Super1" ["Saturn" "Earth"]
                "Super2" {"Sun" {"Halley" :mass}}})
      (are [query] (nil? query) 
           (select "Super1" "Saturn" "Titan" {})
           (select "Super2" "Sun" "Halley" {}))))

(deftest with-cass-test
  (testing 
    "Test change connection"
    (with-cass "Test Cluster" "localhost" 9160 "Keyspace1"
      (let [cf "Standard1", k "Mu Cephei", col :mass, value 15]
        (insert! cf k col value)
        (is (= value (select cf k col)))
        (delete! cf k col)))))
