(ns vhector.internal.dispatch
  (:use
    [vhector.internal.util :only [single?]]
    [vhector.internal.hector :only [read-rows, read-range-rows, read-row-super, read-rows-super, read-range-rows-super]]))

(defn super-dispatcher [& args]
  (let [ks (second args), sups (nth args 2), cols (last args)
        range-keys (map? ks)
        many-sups (or (vector? sups) (map? sups))]
    (cond
      (and many-sups (not= cols {})) :bad-super ; only all cols supported with many supers
      range-keys :range-keys-super
      many-sups :many-sups
      :default :super-one-row)))

(defn dispatcher [& args]
  (let [rows (last (butlast args))
        range-rows (map? rows)
        super (= (count args) 4)]
    (cond 
      (number? (last args)) (apply dispatcher (butlast args)) ; ignore size args at the end for dispatching
      super (apply super-dispatcher args)
      range-rows :range-keys)))

(defmulti dispatch-select dispatcher)

(defn ensure-many [arg]
  (if (single? arg) [arg] arg))

(defmethod dispatch-select :default
  [cf ks cols & more]
  (apply read-rows cf (ensure-many ks) (ensure-many cols) more))

(defmethod dispatch-select :range-keys
  [& args]
  (apply read-range-rows args))

(defmethod dispatch-select :super-one-row
  [cf k super cols & more]
  (apply read-row-super cf k super (ensure-many cols) more))

(defmethod dispatch-select :many-sups
  [cf k super cols & more]
  (apply read-rows-super cf k super cols more))

(defmethod dispatch-select :range-keys-super
  [cf k super cols & more]
  (apply read-range-rows-super cf k super cols more))

(defmethod dispatch-select :bad-super
  [& args]
  (throw (Exception. "All columns {} must be selected when using a list or a range of super columns.")))