// Package obs: PulseMetrics'in KENDI olculeri.
//
// Bir izleme sisteminin en can sikici arizasi sessizce durmasidir: panelde
// grafik duz cizgi olur ve bunun "hic istek gelmedi" mi yoksa "collector
// oldu" mu oldugunu kimse bilemez. Bu yuzden gozlemleyen sistemin de
// gozlemlenmesi gerekir.
//
// Neden Prometheus formati? Cunku onu okuyacak sey PulseMetrics'in kendisi
// olamaz - dairesel bagimlilik olurdu. Collector coktugunde onu haber
// verecek sey BASKA bir sistem olmali. docker-compose'da zaten duran
// Prometheus tam da bu isi yapiyor.
//
// Olculer varsayilan Prometheus kayit defterine yazilir; bu sayede Go
// calisma zamani olculeri (goroutine sayisi, heap, GC duraklamalari) de
// ucretsiz gelir - ki collector'da goroutine sizintisini once orada
// gorursun.
package obs

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/nisah/pulse-metrics/internal/buildinfo"
)

// Sinyal adlari: olculerin "signal" etiketi icin.
const (
	SignalMetrics = "metrics"
	SignalTraces  = "traces"
	SignalLogs    = "logs"
)

// Etiket secimi hakkinda: her etiket degeri ayri bir zaman serisi demek.
// Buradaki etiketler bilerek KAPALI kumeler (signal: 3 deger, table: 8
// deger, result: 2 deger). service_name gibi sinirsiz bir alani etiket
// yapmak, kendi anlattigimiz kardinalite patlamasini kendi uzerimizde
// yapmak olurdu.
var (
	// MessagesConsumed: Kafka'dan okunan mesaj sayisi.
	MessagesConsumed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "pulse_collector_messages_consumed_total",
		Help: "Kafka'dan basariyla okunan mesaj sayisi",
	}, []string{"signal"})

	// RecordsIngested: mesajlarin icinden cikan kayit sayisi (metrik,
	// span, log satiri). Mesaj sayisindan farki: bir mesaj yuzlerce kayit
	// tasiyabilir, asil hacim budur.
	RecordsIngested = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "pulse_collector_records_ingested_total",
		Help: "Kafka mesajlarindan cozulup yazilan kayit sayisi",
	}, []string{"signal"})

	// IngestErrors: kaydetme sirasinda olusan hatalar.
	// kind ayrimi onemli: "decode" bozuk mesaj demektir (uretici hatasi),
	// "read" Kafka sorunu, "write" ScyllaDB sorunu. Ucu ayri mudahale ister.
	IngestErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "pulse_collector_ingest_errors_total",
		Help: "Ingest hatalari (kind: read, decode, write)",
	}, []string{"signal", "kind"})

	// IngestDuration: bir Kafka mesajinin islenme suresi.
	// Bu histogram yavaslamanin nerede oldugunu soyler; artiyorsa
	// ScyllaDB yazma gecikmesi yukselmis demektir.
	IngestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "pulse_collector_ingest_duration_seconds",
		Help:    "Bir Kafka mesajinin cozulup yazilma suresi",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 12), // 1ms .. ~4s
	}, []string{"signal"})

	// KafkaLag: okunmayi bekleyen mesaj sayisi.
	//
	// Uretimdeki EN ONEMLI olcu bu. Surekli artiyorsa collector uretimden
	// yavas kaliyordur ve veri gecikmesi buyuyordur. Collector sayisini
	// artirma karari bu grafige bakilarak verilir.
	KafkaLag = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "pulse_collector_kafka_lag",
		Help: "Kafka'da okunmayi bekleyen mesaj sayisi",
	}, []string{"signal"})

	// AlertEvaluations: degerlendirilen kural sayisi.
	AlertEvaluations = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pulse_alert_evaluations_total",
		Help: "Degerlendirilen alarm kurali sayisi",
	})

	// AlertTransitions: alarm durum gecisleri.
	//
	// result="won"  bu surec gecisi kazandi ve bildirimi o gonderdi
	// result="lost" baska bir collector ayni gecisi once yapti
	//
	// Cok orneklemeli calismada "lost" sayaci sifirdan buyukse sistem
	// dogru calisiyor demektir: ayni alarm icin tek bildirim gitti.
	AlertTransitions = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "pulse_alert_transitions_total",
		Help: "Alarm durum gecisleri (state: firing/resolved, result: won/lost)",
	}, []string{"state", "result"})

	// QueryDuration: gRPC okuma yolunun gecikmesi.
	QueryDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "pulse_query_duration_seconds",
		Help:    "gRPC sorgu suresi",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 12),
	}, []string{"rpc", "status"})

	// buildInfo: her zaman 1 degerinde duran, bilgisi ETIKETLERINDE olan
	// bir olcu. Prometheus dunyasinda surum bildirmenin standart yolu.
	buildInfo = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "pulse_build_info",
		Help: "Calisan ikili dosyanin surumu (deger her zaman 1)",
	}, []string{"version", "commit", "go_version", "component", "instance"})
)

// SetBuildInfo: acilista bir kez cagrilir.
func SetBuildInfo(component, instance string) {
	b := buildinfo.Get()
	buildInfo.WithLabelValues(b.Version, b.Commit, b.GoVersion, component, instance).Set(1)
}

// Handler: /metrics ucu.
func Handler() http.Handler {
	return promhttp.Handler()
}
