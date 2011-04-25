(ns vhector.internal.util)

(defn enmap [arg & more]
  (if more {arg (apply enmap more)} arg))

(defn single? [arg]
  (and (not (nil? arg))
       (not (vector? arg))
       (not (map? arg))))

(defn empty-to-nil [arg]
  (if (= {} arg) nil arg))
  
(defn demap [m & args]
  "Extract value from a map if args target a single element"
  (if (number? (last args)) 
    (apply demap m (butlast args))
    (loop [selectors (rest args), new-m m]
      (if (and  (single? (first selectors)) (not (nil? (first new-m))))
        (recur 
          (rest selectors)
          (val (first new-m)))
        (empty-to-nil new-m)))))