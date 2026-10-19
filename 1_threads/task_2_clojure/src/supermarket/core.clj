(ns supermarket.core
  "Симуляция касс супермаркета на агентах.

  Каждая касса это агент: действия, отправленные агенту через send-off,
  выполняются строго по одному, поэтому очередь действий агента и есть
  очередь покупателей к кассе. Генератор покупателей работает в future
  и ставит покупателя в самую короткую очередь. Общая статистика лежит
  в atom и обновляется через swap!."
  (:gen-class)
  (:import (java.util Random)
           (java.util.concurrent.locks LockSupport)))

;; Модельное время: 1 секунда модели = 0.25 мс реального времени.
(def ^:const nanos-per-sim-sec 250000)

;; Время обслуживания: подготовка и оплата + время на каждый товар (секунды).
(def ^:const base-service-sec 15.0)
(def ^:const per-item-sec 3.0)
(def ^:const max-items 30)

(defn sim-now
  "Текущее модельное время в секундах от момента t0 (в нс)."
  [t0]
  (/ (- (System/nanoTime) t0) (double nanos-per-sim-sec)))

(defn sim-sleep
  "Ждёт заданное число модельных секунд."
  [sec]
  (let [deadline (+ (System/nanoTime) (long (* sec nanos-per-sim-sec)))]
    (loop []
      (let [left (- deadline (System/nanoTime))]
        (when (pos? left)
          (LockSupport/parkNanos left)
          (recur))))))

(defn service-time [items]
  (+ base-service-sec (* per-item-sec items)))

(defn make-customers
  "Заранее генерирует покупателей: паузу перед появлением (экспоненциальное
  распределение со средним 1/rate) и число товаров. Генератор с фиксированным
  зерном даёт один и тот же поток покупателей для любого числа касс."
  [n rate seed]
  (let [rnd (Random. seed)]
    (vec (for [i (range n)]
           {:id i
            :gap (/ (- (Math/log (- 1.0 (.nextDouble rnd)))) rate)
            :items (inc (.nextInt rnd max-items))}))))

(defn shortest-queue
  "Индекс самой короткой очереди (при равенстве первая)."
  [qs]
  (reduce (fn [best i] (if (< (qs i) (qs best)) i best))
          0 (range (count qs))))

(defn serve
  "Действие агента-кассы: обслужить одного покупателя."
  [register customer {:keys [t0 queues stats]}]
  (let [start (sim-now t0)
        wait (- start (:arrival customer))]
    (sim-sleep (service-time (:items customer)))
    (let [busy (- (sim-now t0) start)]
      (swap! queues update (:id register) dec)
      (swap! stats (fn [s]
                     (-> s
                         (update :served inc)
                         (update :total-wait + wait)
                         (update :max-wait max wait))))
      (-> register
          (update :served inc)
          (update :busy + busy)))))

(defn simulate
  "Прогоняет поток покупателей через k касс и возвращает итоги."
  [k customers]
  (let [registers (mapv #(agent {:id % :served 0 :busy 0.0}) (range k))
        queues (atom (vec (repeat k 0)))
        stats (atom {:served 0 :total-wait 0.0 :max-wait 0.0 :max-queue 0})
        t0 (System/nanoTime)
        ctx {:t0 t0 :queues queues :stats stats}
        generator
        (future
          (doseq [c customers]
            (sim-sleep (:gap c))
            ;; swap-vals! возвращает очереди до изменения, по ним однозначно
            ;; определяется касса, в которую попал покупатель.
            (let [[before after] (swap-vals! queues #(update % (shortest-queue %) inc))
                  idx (shortest-queue before)]
              (swap! stats update :max-queue max (after idx))
              (send-off (registers idx) serve
                        (assoc c :arrival (sim-now t0)) ctx))))]
    @generator
    (apply await registers)
    (doseq [r registers]
      (when-let [e (agent-error r)]
        (throw (ex-info "касса упала" {:id (:id @r)} e))))
    {:k k
     :total-time (sim-now t0)
     :stats @stats
     :registers (mapv deref registers)}))

(defn print-result [{:keys [k total-time stats registers]} real-sec]
  (let [{:keys [served total-wait max-wait max-queue]} stats]
    (println (format "Касс: %d" k))
    (println (format "  обслужено покупателей: %d, время работы магазина: %.0f с (%.1f мин)"
                     served total-time (/ total-time 60)))
    (println (format "  ожидание в очереди: среднее %.1f с, максимальное %.1f с"
                     (/ total-wait (max served 1)) max-wait))
    (println (format "  самая длинная очередь: %d чел." max-queue))
    (println "  касса  обслужено  загрузка")
    (doseq [{:keys [id served busy]} registers]
      (println (format "  %5d  %9d  %7.1f %%" (inc id) served (* 100 (/ busy total-time)))))
    (println (format "  (реальное время прогона: %.2f с)" real-sec))
    (println)))

(defn parse-args [args]
  (let [[k n rate] args]
    {:max-registers (if k (Integer/parseInt k) 5)
     :customers (if n (Integer/parseInt n) 300)
     :rate (if rate (Double/parseDouble rate) 0.1)}))

(defn -main [& args]
  (let [{:keys [max-registers customers rate]}
        (try (parse-args args)
             (catch NumberFormatException _
               (println "использование: clojure -M -m supermarket.core [касс] [покупателей] [покупателей в секунду]")
               (System/exit 1)))]
    (when (or (< max-registers 1) (< customers 1) (not (pos? rate)))
      (println "все параметры должны быть положительными")
      (System/exit 1))
    (println (format "Покупателей: %d, в среднем %.3f покупателя в секунду (один раз в %.1f с)"
                     customers rate (/ 1 rate)))
    (println (format "Обслуживание: %.0f с + %.0f с на товар, товаров от 1 до %d"
                     base-service-sec per-item-sec max-items))
    (println (format "Масштаб: 1 с модели = %.2f мс реального времени" (/ nanos-per-sim-sec 1e6)))
    (println)
    (let [cs (make-customers customers rate 42)]
      (doseq [k (range 1 (inc max-registers))]
        (let [start (System/nanoTime)
              result (simulate k cs)]
          (print-result result (/ (- (System/nanoTime) start) 1e9)))))
    (shutdown-agents)))
