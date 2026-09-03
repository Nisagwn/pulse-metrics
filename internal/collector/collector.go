package collector

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/gocql/gocql"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/nisah/pulse-metrics/internal/buildinfo"
	"github.com/nisah/pulse-metrics/internal/config"
	"github.com/nisah/pulse-metrics/internal/health"
	"github.com/nisah/pulse-metrics/internal/obs"
	pb "github.com/nisah/pulse-metrics/pkg/pulsepb"
)

// Config: collector configuration
type Config struct {
	KafkaBrokers []string
	ScyllaAddr   string
	GRPCPort     string
	HealthAddr   string
	Topic        string // bos ise "pulse-metrics"
	GroupID      string // bos ise "pulse-collector"

	// Scylla: Faz 4 baglanti/replikasyon ayarlari. nil birakilirsa
	// ScyllaAddr'dan gelistirme varsayilanlariyla turetilir - boylece
	// mevcut cagiranlar ve testler degismeden calismaya devam eder.
	Scylla *config.Scylla

	// InstanceID: bu collector'in kumedeki adi. Bos ise hostname.
	// Faz 4'te sart oldu: paylasilan alarm durumunda "gecisi kim yapti"
	// sorusunun cevabi bu.
	InstanceID string

	// Trace tarafi (Faz 2). Ayri topic ve ayri consumer group:
	// trace tuketimi yavaslarsa metrik tuketimi etkilenmemeli.
	TracesTopic   string // bos ise "pulse-traces"
	TracesGroupID string // bos ise "pulse-collector-traces"
	DisableTraces bool   // testlerde trace tuketimini kapatmak icin

	// Log ve alarm tarafi (Faz 3).
	LogsTopic     string // bos ise "pulse-logs"
	LogsGroupID   string // bos ise "pulse-collector-logs"
	DisableLogs   bool
	DisableAlerts bool
	AlertInterval time.Duration // bos ise 30s

	Debug bool
}

// Collector: metrics collector server
type Collector struct {
	config      *Config
	logger      *zap.Logger
	session     *gocql.Session
	reader      *kafka.Reader
	traceReader *kafka.Reader
	logReader   *kafka.Reader
	alerts      *AlertEngine
	started     time.Time
	instanceID  string

	mu      sync.Mutex
	stopped bool
}

// scyllaConfig: Config.Scylla verilmediyse eski tek adresli alandan
// gelistirme varsayilanlarini turetir.
func (cfg *Config) scyllaConfig() config.Scylla {
	if cfg.Scylla != nil {
		return *cfg.Scylla
	}
	addr := cfg.ScyllaAddr
	if addr == "" {
		addr = "localhost:9042"
	}
	return config.Scylla{
		Hosts:             []string{addr},
		Keyspace:          "pulse",
		Consistency:       "QUORUM",
		ReplicationClass:  "SimpleStrategy",
		ReplicationFactor: 1,
		Timeout:           10 * time.Second,
		ConnectTimeout:    15 * time.Second,
		NumConns:          2,
	}
}

// Kafka varsayilanlari. Testler kendi topic/group'lariyla izole calisabilsin
// diye Config uzerinden degistirilebilir.
const (
	DefaultTopic   = "pulse-metrics"
	DefaultGroupID = "pulse-collector"
)

// MetricsTableDDL: metrics tablosunun Faz 4 semasi.
//
// Faz 1'den beri tasinan borc burada kapaniyor: partition key'e saatlik
// time_bucket eklendi.
//
//	Faz 1-3: PRIMARY KEY ((service_name, metric_name), timestamp, instance_id)
//	Faz 4:   PRIMARY KEY ((service_name, metric_name, time_bucket), timestamp, instance_id)
//
// Eski halinde bir metrigin TUM gecmisi tek partition'daydi ve TTL doluncaya
// kadar durmadan buyuyordu. 10 saniyede bir olcum gonderen 10 instance,
// 30 gunde tek partition'da ~2.6 milyon satir demek. Cassandra/Scylla'da
// bu, o partition'i tutan dugumu sicak nokta haline getirir; okuma
// gecikmesi ve compaction maliyeti partition boyutuyla birlikte buyur.
//
// Saatlik kova bunu sinirlar: partition basina en fazla bir saatlik veri.
// Bedeli, "son 24 saat" sorgusunun 24 partition okumasi - ki Cassandra
// icin bu tamamen olagan bir erisim bicimidir. Gunluk kova daha az
// partition okurdu ama tek partition'i 24 kat buyuturdu; bu takas
// bilincli olarak saatlik lehine yapildi ve projedeki diger tablolarla
// (logs, trace_index, alerts) tutarli.
const MetricsTableDDL = `
	CREATE TABLE IF NOT EXISTS pulse.metrics (
		service_name TEXT,
		metric_name  TEXT,
		time_bucket  TEXT,
		timestamp    BIGINT,
		instance_id  TEXT,
		type         TEXT,
		tags         MAP<TEXT, TEXT>,
		labels       MAP<TEXT, TEXT>,
		value        DOUBLE,
		PRIMARY KEY ((service_name, metric_name, time_bucket), timestamp, instance_id)
	) WITH CLUSTERING ORDER BY (timestamp DESC, instance_id ASC)
		AND default_time_to_live = 2592000`

const insertMetric = `
	INSERT INTO metrics (service_name, metric_name, time_bucket, timestamp, instance_id, type, tags, labels, value)
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`

// ensureSchema: keyspace ve tabloyu, keyspace'e BAGLANMADAN once yaratir.
//
// Bu ayrim onemli. Onceki surum once cluster.Keyspace = "pulse" diyip
// oturum aciyor, sonra keyspace'i yaratmaya calisiyordu; bos bir ScyllaDB'de
// ilk acilis "keyspace does not exist" ile basarisiz olurdu. Cozum, DDL'i
// keyspace belirtmeyen ayri ve kisa omurlu bir oturumdan calistirmak.
func ensureSchema(sc config.Scylla, logger *zap.Logger) error {
	cluster := gocql.NewCluster(sc.Hosts...)
	cluster.Consistency = gocql.Quorum
	cluster.Timeout = sc.Timeout
	cluster.ConnectTimeout = sc.ConnectTimeout
	// Bilerek Keyspace atanmiyor: henuz var olmayabilir.

	session, err := cluster.CreateSession()
	if err != nil {
		return fmt.Errorf("bootstrap oturumu acilamadi: %w", err)
	}
	defer session.Close()

	keyspaceDDL := fmt.Sprintf("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = %s",
		sc.Keyspace, sc.ReplicationCQL())
	if err := session.Query(keyspaceDDL).Exec(); err != nil {
		return fmt.Errorf("keyspace yaratilamadi: %w", err)
	}
	if err := session.Query(MetricsTableDDL).Exec(); err != nil {
		return fmt.Errorf("metrics tablosu yaratilamadi: %w", err)
	}

	// CREATE TABLE IF NOT EXISTS var olan bir tabloyu DEGISTIRMEZ; sessizce
	// hicbir sey yapar. Faz 3'ten kalan eski semali bir tablo varsa collector
	// acilir, sonra her INSERT "undefined column time_bucket" ile patlar ve
	// veri sessizce kaybolur. Bu yuzden semayi acilista DOGRULUYORUZ.
	if err := verifyMetricsSchema(session, sc.Keyspace); err != nil {
		return err
	}

	logger.Info("Schema ensured",
		zap.String("keyspace", sc.Keyspace),
		zap.String("replication", sc.ReplicationCQL()))
	return nil
}

// verifyMetricsSchema: calisan tablonun partition key'i kodun bekledigiyle
// ayni mi? Degilse acilisi durdurur.
//
// Neden acilisi durduruyoruz? Cunku alternatif daha kotu: yanlis semayla
// acilan bir collector orkestratore "saglikliyim" der, Kafka'dan okumaya
// devam eder ve okudugu her mesaji yazamadan atar. Sessiz veri kaybi,
// gurultulu bir acilis hatasindan her zaman daha pahalidir.
func verifyMetricsSchema(session *gocql.Session, keyspace string) error {
	iter := session.Query(`
		SELECT column_name, kind FROM system_schema.columns
		WHERE keyspace_name = ? AND table_name = 'metrics'`, keyspace).Iter()

	var (
		name, kind string
		partition  []string
		found      bool
	)
	for iter.Scan(&name, &kind) {
		found = true
		if kind == "partition_key" {
			partition = append(partition, name)
		}
	}
	if err := iter.Close(); err != nil {
		return fmt.Errorf("sema dogrulanamadi: %w", err)
	}
	if !found {
		return fmt.Errorf("metrics tablosu bulunamadi")
	}

	for _, col := range partition {
		if col == "time_bucket" {
			return nil
		}
	}

	return fmt.Errorf(
		"metrics tablosu ESKI semada (partition key: %s). Faz 4 time_bucket bekliyor.\n"+
			"  Gocu calistir:  go run ./cmd/pulse-migrate -scylla %s -confirm\n"+
			"  Ayrintilar:     docs/OPERATIONS.md",
		strings.Join(partition, ", "), keyspace)
}

// NewCollector: create collector
func NewCollector(cfg *Config) (*Collector, error) {
	var logger *zap.Logger
	var err error

	if cfg.Debug {
		logger, err = zap.NewDevelopment()
	} else {
		logger, err = zap.NewProduction()
	}
	if err != nil {
		return nil, fmt.Errorf("failed to create logger: %w", err)
	}

	sc := cfg.scyllaConfig()
	if err := sc.Validate(); err != nil {
		return nil, fmt.Errorf("scylla ayarlari gecersiz: %w", err)
	}

	instanceID := config.InstanceID(cfg.InstanceID)

	logger.Info("Collector starting",
		zap.String("version", buildinfo.Get().String()),
		zap.String("instance", instanceID))

	// Once sema, sonra asil oturum.
	if err := ensureSchema(sc, logger); err != nil {
		return nil, err
	}

	cluster := gocql.NewCluster(sc.Hosts...)
	cluster.Keyspace = sc.Keyspace
	cluster.Timeout = sc.Timeout
	cluster.ConnectTimeout = sc.ConnectTimeout
	cluster.NumConns = sc.NumConns

	consistency, err := gocql.ParseConsistencyWrapper(strings.ToUpper(sc.Consistency))
	if err != nil {
		return nil, fmt.Errorf("consistency cozulemedi: %w", err)
	}
	cluster.Consistency = consistency

	// LOCAL_QUORUM'un anlamli olmasi icin surucunun hangi DC'de oldugunu
	// bilmesi gerekir; aksi halde "yerel" diye uzak dugumlere sorabilir.
	if sc.LocalDC != "" {
		cluster.PoolConfig.HostSelectionPolicy =
			gocql.DCAwareRoundRobinPolicy(sc.LocalDC)
	}

	session, err := cluster.CreateSession()
	if err != nil {
		return nil, fmt.Errorf("failed to connect to ScyllaDB: %w", err)
	}

	// Trace tablolari (Faz 2). Keyspace artik var, bu yuzden asil
	// oturumdan yaratilabilirler.
	if err := ensureTraceSchema(session, logger); err != nil {
		session.Close()
		return nil, err
	}

	// Log, kenar ve alarm tablolari (Faz 3).
	if err := ensurePhase3Schema(session, logger); err != nil {
		session.Close()
		return nil, err
	}

	// Paylasilan alarm durumu (Faz 4).
	if err := ensurePhase4Schema(session, logger); err != nil {
		session.Close()
		return nil, err
	}

	topic := cfg.Topic
	if topic == "" {
		topic = DefaultTopic
	}
	groupID := cfg.GroupID
	if groupID == "" {
		groupID = DefaultGroupID
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        cfg.KafkaBrokers,
		Topic:          topic,
		GroupID:        groupID,
		CommitInterval: 1 * time.Second,
		StartOffset:    kafka.FirstOffset,
	})

	var traceReader *kafka.Reader
	if !cfg.DisableTraces {
		traceReader = newTraceReader(cfg)
	}

	var logReader *kafka.Reader
	if !cfg.DisableLogs {
		logReader = newLogReader(cfg)
	}

	var engine *AlertEngine
	if !cfg.DisableAlerts {
		engine = NewAlertEngine(session, logger, cfg.AlertInterval, instanceID)
	}

	obs.SetBuildInfo("collector", instanceID)

	return &Collector{
		config:      cfg,
		logger:      logger,
		session:     session,
		reader:      reader,
		traceReader: traceReader,
		logReader:   logReader,
		alerts:      engine,
		started:     time.Now(),
		instanceID:  instanceID,
	}, nil
}

// InstanceID: bu collector'in kumedeki adi.
func (c *Collector) InstanceID() string { return c.instanceID }

// Session: saglik kontrolu ve testler icin acik oturumu verir.
func (c *Collector) Session() *gocql.Session { return c.session }

// Logger: cagiranin ayni logger'i kullanabilmesi icin.
func (c *Collector) Logger() *zap.Logger { return c.logger }

// Ping: ScyllaDB hala erisilebilir mi? /readyz bunu kullanir.
func (c *Collector) Ping(ctx context.Context) error {
	return c.session.Query("SELECT release_version FROM system.local").
		WithContext(ctx).Exec()
}

// Start: uc is parcasini paralel calistirir ve ilki hata verdiginde hepsini durdurur:
//   - Kafka tuketici dongusu (asil is)
//   - gRPC sorgu sunucusu (dashboard-api buradan okur)
//   - HTTP saglik sunucusu
//
// ctx iptal edildiginde (Ctrl+C) hepsi duzgunce kapanir.
func (c *Collector) Start(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	c.logger.Info("Collector started",
		zap.String("scylla", c.config.ScyllaAddr),
		zap.String("grpc", c.config.GRPCPort),
		zap.String("health", c.config.HealthAddr),
	)

	errCh := make(chan error, 7)
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		errCh <- c.reportKafkaLag(ctx)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		errCh <- c.consume(ctx)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		errCh <- c.consumeTraces(ctx)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		errCh <- c.consumeLogs(ctx)
	}()

	if c.alerts != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errCh <- c.alerts.Run(ctx)
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		errCh <- c.serveGRPC(ctx)
	}()

	hs := health.New(c.config.HealthAddr, c.logger)
	hs.AddCheck("scylladb", c.Ping)
	wg.Add(1)
	go func() {
		defer wg.Done()
		errCh <- hs.Serve(ctx)
	}()

	// Ilk biten (hata veren ya da temiz duran) digerlerini de durdurur.
	err := <-errCh
	cancel()
	wg.Wait()

	if closeErr := c.Close(); closeErr != nil && err == nil {
		err = closeErr
	}
	return err
}

// serveGRPC: MetricsService'i dinlemeye alir. Log'un soyledigi seyi
// gercekten yapan kod burasi.
func (c *Collector) serveGRPC(ctx context.Context) error {
	lis, err := net.Listen("tcp", ":"+c.config.GRPCPort)
	if err != nil {
		return fmt.Errorf("gRPC portu dinlenemedi: %w", err)
	}

	srv := grpc.NewServer()
	pb.RegisterMetricsServiceServer(srv, NewMetricsService(c.session, c.logger, c.started))
	pb.RegisterTraceServiceServer(srv, NewTraceService(c.session, c.logger))
	pb.RegisterLogServiceServer(srv, NewLogServiceServer(c.session, c.logger))
	pb.RegisterAlertServiceServer(srv, NewAlertServiceServer(c.session, c.logger, c.alerts))

	go func() {
		<-ctx.Done()
		// GracefulStop bekleyen RPC'lerin bitmesini bekler.
		srv.GracefulStop()
	}()

	c.logger.Info("gRPC server listening", zap.String("port", c.config.GRPCPort))
	if err := srv.Serve(lis); err != nil {
		return fmt.Errorf("gRPC sunucusu durdu: %w", err)
	}
	return nil
}

// consume: Kafka'dan okuyup ScyllaDB'ye yazan asil dongu.
func (c *Collector) consume(ctx context.Context) error {
	for {
		msg, err := c.reader.ReadMessage(ctx)
		if err != nil {
			// ctx iptal edildiyse bu bir hata degil, kapanma sinyalidir.
			if errors.Is(err, context.Canceled) || errors.Is(ctx.Err(), context.Canceled) {
				c.logger.Info("Consumer loop stopping")
				return nil
			}
			c.logger.Error("Failed to read message", zap.Error(err))

			// Yeniden denemeden once bekle, ama beklerken de iptali dinle.
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(1 * time.Second):
			}
			continue
		}

		obs.MessagesConsumed.WithLabelValues(obs.SignalMetrics).Inc()
		began := time.Now()

		var payload pb.MetricsPayload
		if err := proto.Unmarshal(msg.Value, &payload); err != nil {
			// Bozuk mesaj kuyrugu tikamamali: logla ve devam et.
			obs.IngestErrors.WithLabelValues(obs.SignalMetrics, "decode").Inc()
			c.logger.Error("Failed to unmarshal metrics", zap.Error(err))
			continue
		}

		if err := c.storeMetrics(ctx, &payload); err != nil {
			obs.IngestErrors.WithLabelValues(obs.SignalMetrics, "write").Inc()
			c.logger.Error("Failed to store metrics",
				zap.Error(err),
				zap.String("service", payload.ServiceName),
			)
		}
		obs.IngestDuration.WithLabelValues(obs.SignalMetrics).Observe(time.Since(began).Seconds())
	}
}

// reportKafkaLag: consumer group'larin gecikmesini Prometheus'a yazar.
//
// Uretimde bakilacak ilk grafik budur. Lag surekli artiyorsa collector
// uretimden yavas kaliyordur; cozum ya daha fazla collector ya da daha
// fazla partition'dir (ikisi birlikte - partition sayisi tuketici
// paralelligini yukaridan sinirlar).
//
// NEDEN Reader.Lag() DEGIL?
//
// Ilk surum kafka-go'nun Reader.Lag() metodunu kullaniyordu ve olcu
// surekli -1 donuyordu. Sebep kutuphanenin belgesinde yaziyor: consumer
// group modundaki bir okuyucu icin Lag() tanimsizdir ve -1 doner, cunku
// okuyucu birden fazla partition'a hizmet eder ve tek bir "gecikme"
// degeri yoktur.
//
// Dogrusu iki soruyu broker'a sormak:
//  1. her partition'in SON offset'i nerede?      (ListOffsets)
//  2. grup nereye kadar commit etti?             (OffsetFetch)
//
// Ikisinin farkinin toplami, grubun o topic'teki gercek gecikmesi.
//
// Bu, tek bir collector'in degil GRUBUN gecikmesini olcer - dogru olan
// da bu: iki collector partition'lari bolustugunde onemli olan toplam
// birikmedir, kimin ne kadarini isledigi degil.
func (c *Collector) reportKafkaLag(ctx context.Context) error {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	client := &kafka.Client{
		Addr:    kafka.TCP(c.config.KafkaBrokers...),
		Timeout: 10 * time.Second,
	}

	type consumer struct {
		topic string
		group string
	}
	groups := map[string]consumer{
		obs.SignalMetrics: {topicOrDefault(c.config.Topic, DefaultTopic),
			groupOrDefault(c.config.GroupID, DefaultGroupID)},
		obs.SignalTraces: {topicOrDefault(c.config.TracesTopic, DefaultTracesTopic),
			groupOrDefault(c.config.TracesGroupID, DefaultTracesGroupID)},
		obs.SignalLogs: {topicOrDefault(c.config.LogsTopic, DefaultLogsTopic),
			groupOrDefault(c.config.LogsGroupID, DefaultLogsGroupID)},
	}

	readers := map[string]*kafka.Reader{
		obs.SignalMetrics: c.reader,
		obs.SignalTraces:  c.traceReader,
		obs.SignalLogs:    c.logReader,
	}

	// Olculeri SIFIRLA baslat.
	//
	// Etiketli bir olcu, o etiketle ilk kez yazilana kadar /metrics
	// ciktisinda HIC gorunmez. Ilk tik 15 saniye sonra oldugu icin
	// ilk kazima bos donuyordu ve Prometheus tarafinda "seri yok" ile
	// "deger sifir" birbirinden ayirt edilemiyordu - rate() ve alarm
	// kurallari eksik seri karsisinda sessizce hicbir sey yapar.
	// Bilinen etiketleri acilista sifirlamak standart pratiktir.
	for signal, rd := range readers {
		if rd == nil {
			continue
		}
		obs.KafkaLag.WithLabelValues(signal).Set(0)
		obs.MessagesConsumed.WithLabelValues(signal).Add(0)
		obs.RecordsIngested.WithLabelValues(signal).Add(0)
		for _, kind := range []string{"read", "decode", "write"} {
			obs.IngestErrors.WithLabelValues(signal, kind).Add(0)
		}
	}

	measure := func() {
		for signal, rd := range readers {
			if rd == nil {
				continue
			}
			g := groups[signal]
			lag, err := consumerGroupLag(ctx, client, g.topic, g.group)
			if err != nil {
				// Gecikme olculemedi. Bu bir ingest hatasi degil, sadece
				// gozlem eksikligi: olcuyu OLDUGU GIBI birakiyoruz ki
				// grafikte sahte bir sifir belirmesin.
				c.logger.Debug("Kafka gecikmesi olculemedi",
					zap.String("signal", signal), zap.Error(err))
				continue
			}
			obs.KafkaLag.WithLabelValues(signal).Set(float64(lag))
		}
	}

	measure() // ilk kazimayi 15 saniye bekletme

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			measure()
		}
	}
}

// consumerGroupLag: bir consumer group'un bir topic'teki toplam gecikmesi.
//
// lag = SUM(partition son offset - grubun commit ettigi offset)
func consumerGroupLag(ctx context.Context, client *kafka.Client, topic, group string) (int64, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	meta, err := client.Metadata(ctx, &kafka.MetadataRequest{Topics: []string{topic}})
	if err != nil {
		return 0, fmt.Errorf("topic bilgisi alinamadi: %w", err)
	}

	var (
		partitions   []int
		offsetReqs   []kafka.OffsetRequest
		foundPartion bool
	)
	for _, t := range meta.Topics {
		if t.Name != topic {
			continue
		}
		for _, p := range t.Partitions {
			partitions = append(partitions, p.ID)
			offsetReqs = append(offsetReqs, kafka.LastOffsetOf(p.ID))
			foundPartion = true
		}
	}
	if !foundPartion {
		return 0, fmt.Errorf("topic %q bulunamadi", topic)
	}

	last, err := client.ListOffsets(ctx, &kafka.ListOffsetsRequest{
		Topics: map[string][]kafka.OffsetRequest{topic: offsetReqs},
	})
	if err != nil {
		return 0, fmt.Errorf("son offset'ler alinamadi: %w", err)
	}

	committed, err := client.OffsetFetch(ctx, &kafka.OffsetFetchRequest{
		GroupID: group,
		Topics:  map[string][]int{topic: partitions},
	})
	if err != nil {
		return 0, fmt.Errorf("commit edilen offset'ler alinamadi: %w", err)
	}
	if committed.Error != nil {
		return 0, fmt.Errorf("grup offset hatasi: %w", committed.Error)
	}

	committedByPartition := make(map[int]int64, len(partitions))
	for _, p := range committed.Topics[topic] {
		if p.Error != nil {
			continue
		}
		committedByPartition[p.Partition] = p.CommittedOffset
	}

	var total int64
	for _, p := range last.Topics[topic] {
		if p.Error != nil {
			continue
		}
		c, ok := committedByPartition[p.Partition]
		if !ok || c < 0 {
			// Grup bu partition'a hic commit etmemis: tum partition
			// islenmeyi bekliyor demektir.
			c = p.FirstOffset
		}
		if d := p.LastOffset - c; d > 0 {
			total += d
		}
	}
	return total, nil
}

func topicOrDefault(v, def string) string {
	if v == "" {
		return def
	}
	return v
}

func groupOrDefault(v, def string) string {
	if v == "" {
		return def
	}
	return v
}

// storeMetrics: persist metrics to ScyllaDB
//
// Her metrik ayri bir INSERT. Bilerek BATCH kullanmiyoruz: bir payload'daki
// metriklerin metric_name'leri farkli, yani farkli partition'lara dusuyorlar.
// Cassandra/Scylla'da coklu partition BATCH'i hizlandirmaz, aksine
// koordinator dugumu uzerinde darbogaz yaratir.
func (c *Collector) storeMetrics(ctx context.Context, payload *pb.MetricsPayload) error {
	if payload == nil || len(payload.Metrics) == 0 {
		return nil
	}

	instanceID := payload.InstanceId
	if instanceID == "" {
		// Bos instance_id clustering key'i ise yaramaz hale getirir;
		// en azindan ayirt edilebilir bir deger koy.
		instanceID = "unknown"
	}

	for _, metric := range payload.Metrics {
		if metric.Name == "" {
			c.logger.Warn("Skipping metric with empty name",
				zap.String("service", payload.ServiceName))
			continue
		}

		ts := metric.TimestampMillis
		if ts == 0 && payload.Timestamp != nil {
			ts = payload.Timestamp.AsTime().UnixMilli()
		}

		if err := c.session.Query(insertMetric,
			payload.ServiceName,
			metric.Name,
			// Kova, olcumun KENDI zamanindan turetilir; yazma zamanindan
			// degil. Gec gelen bir olcum ait oldugu saatin partition'ina
			// yazilmali, yoksa zaman araligi sorgulari onu kacirir.
			TimeBucket(time.UnixMilli(ts)),
			ts,
			instanceID,
			metric.Type.String(), // GAUGE / COUNTER / HISTOGRAM / SUMMARY
			metric.Tags,
			metric.Labels,
			metric.Value,
		).WithContext(ctx).Exec(); err != nil {
			return fmt.Errorf("failed to insert metric %q: %w", metric.Name, err)
		}
		obs.RecordsIngested.WithLabelValues(obs.SignalMetrics).Inc()
	}

	if c.config.Debug {
		c.logger.Debug("Metrics stored",
			zap.String("service", payload.ServiceName),
			zap.String("instance", instanceID),
			zap.Int("count", len(payload.Metrics)),
		)
	}

	return nil
}

// Close: gracefully shutdown collector
func (c *Collector) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.stopped {
		return nil
	}
	c.stopped = true

	// Uc okuyucuyu PARALEL kapatiyoruz. Her biri consumer group'tan
	// ayrilirken saniyeler surebiliyor; sirayla kapatmak kapanis suresini
	// gereksiz yere ucler.
	readers := map[string]*kafka.Reader{
		"metrics": c.reader,
		"traces":  c.traceReader,
		"logs":    c.logReader,
	}

	var (
		closeWg  sync.WaitGroup
		errMu    sync.Mutex
		firstErr error
	)
	for name, rd := range readers {
		if rd == nil {
			continue
		}
		closeWg.Add(1)
		go func(name string, rd *kafka.Reader) {
			defer closeWg.Done()
			if err := rd.Close(); err != nil {
				c.logger.Error("Kafka okuyucusu kapatilamadi",
					zap.String("reader", name), zap.Error(err))
				errMu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				errMu.Unlock()
			}
		}(name, rd)
	}
	closeWg.Wait()

	if c.session != nil {
		c.session.Close()
	}

	c.logger.Info("Collector stopped gracefully")

	// Sync en sona: bundan sonra yazilan loglar diske ulasmayabilir.
	// Windows'ta stderr'e Sync ENOTTY dondurur, bu bir hata degil.
	_ = c.logger.Sync()

	return firstErr
}
