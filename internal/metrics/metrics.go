package metrics

import (
	"log"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Глобальные счетчики для статистики (atomic операции)
var TxPackets uint64
var TxBytes uint64

// Prometheus метрики для мониторинга работы системы
var (
	// Счетчик отправленных пакетов
	PromTxPackets = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "streamer_tx_packets_total",
		Help: "Total transmitted packets",
	})
	// Счетчик отправленных байт
	PromTxBytes = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "streamer_tx_bytes_total",
		Help: "Total transmitted bytes",
	})
	// Счетчик принятых пакетов
	PromRxPackets = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "streamer_rx_packets_total",
		Help: "Total received packets",
	})
	// Счетчик пакетов восстановленных с помощью FEC
	PromFECRecovered = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "streamer_fec_recovered_total",
		Help: "Total FEC recovered packets",
	})
	// Счетчик RETX запросов
	PromRetxRequests = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "streamer_retx_requests_total",
		Help: "Total retransmission requests",
	})
	// Счетчик поздних (проигнорированных) пакетов
	PromLatePackets = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "streamer_late_packets_total",
		Help: "Total late packets ignored",
	})
	// Счетчик активных UDP портов
	PromActivePorts = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "streamer_active_ports",
		Help: "Number of active UDP ports",
	})
)

// startPrometheus запускает HTTP сервер с Prometheus метриками
func StartPrometheus(addr string) {
	prometheus.MustRegister(PromTxPackets, PromTxBytes, PromRxPackets, PromFECRecovered, PromRetxRequests, PromLatePackets, PromActivePorts)
	http.Handle("/metrics", promhttp.Handler())

	go func() {
		log.Printf("prometheus: listening on %s", addr)
		if err := http.ListenAndServe(addr, nil); err != nil {
			log.Printf("prometheus serve error: %v", err)
		}
	}()
}