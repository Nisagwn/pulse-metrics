//go:build integration

// Uctan uca entegrasyon testleri: agent -> Kafka -> collector -> ScyllaDB -> gRPC.
//
// Calisan Kafka ve ScyllaDB gerektirir, bu yuzden "integration" build tag'i
// arkasinda. Normal "go test ./..." bu dosyayi hic derlemez.
//
//	docker compose up -d
//	go test -tags integration -v ./test/...
//
// Her test kendi Kafka topic'ini, kendi consumer group'unu ve benzersiz bir
// servis adini kullanir; bu sayede makinede zaten calisan bir collector varken
// de guvenle kosar.
package integration

import (
	"context"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/segmentio/kafka-go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/nisah/pulse-metrics/internal/agent"
	"github.com/nisah/pulse-metrics/internal/collector"
	pb "github.com/nisah/pulse-metrics/internal/proto"
)

const (
	kafkaAddr  = "localhost:9092"
	scyllaAddr = "localhost:9042"
	waitFor    = 90 * time.Second
)

// --- yardimcilar ---------------------------------------------------------

func freePort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("bos port bulunamadi: %v", err)
	}
	defer func() { _ = l.Close() }()
	return strconv.Itoa(l.Addr().(*net.TCPAddr).Port)
}

func createTopic(t *testing.T, topic string) {
	t.Helper()

	conn, err := kafka.Dial("tcp", kafkaAddr)
	if err != nil {
		t.Fatalf("Kafka'ya baglanilamadi (docker compose up -d calistirdin mi?): %v", err)
	}
	defer func() { _ = conn.Close() }()

	ctrl, err := conn.Controller()
	if err != nil {
		t.Fatalf("Kafka controller alinamadi: %v", err)
	}

	ctrlConn, err := kafka.Dial("tcp", net.JoinHostPort(ctrl.Host, strconv.Itoa(ctrl.Port)))
	if err != nil {
		t.Fatalf("controller'a baglanilamadi: %v", err)
	}
	defer func() { _ = ctrlConn.Close() }()

	if err := ctrlConn.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     1,
		ReplicationFactor: 1,
	}); err != nil {
		t.Fatalf("topic yaratilamadi: %v", err)
	}

	t.Cleanup(func() {
		c, err := kafka.Dial("tcp", net.JoinHostPort(ctrl.Host, strconv.Itoa(ctrl.Port)))
		if err != nil {
			return
		}
		defer func() { _ = c.Close() }()
		_ = c.DeleteTopics(topic)
	})

	// CreateTopics hemen doner ama topic'in broker metadata'sina yayilmasi
	// biraz surer. Beklemezsek ilk yazma "Unknown Topic Or Partition" alir.
	waitForTopic(t, topic)
}

func waitForTopic(t *testing.T, topic string) {
	t.Helper()

	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := kafka.Dial("tcp", kafkaAddr)
		if err == nil {
			parts, err := conn.ReadPartitions(topic)
			_ = conn.Close()
			// Sadece topic'in var olmasi yetmez; partition liderinin
			// secilmis olmasi da gerekiyor, yoksa yazma reddedilir.
			if err == nil && len(parts) > 0 && parts[0].Leader.Host != "" {
				return
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("topic %q 30 sn icinde broker metadata'sinda gorunmedi", topic)
}

// scyllaSession: dogrulama ve temizlik icin ayri bir oturum.
func scyllaSession(t *testing.T) *gocql.Session {
	t.Helper()

	cluster := gocql.NewCluster(scyllaAddr)
	cluster.Keyspace = "pulse"
	cluster.Consistency = gocql.Quorum
	cluster.Timeout = 10 * time.Second

	sess, err := cluster.CreateSession()
	if err != nil {
		t.Fatalf("ScyllaDB'ye baglanilamadi: %v", err)
	}
	t.Cleanup(sess.Close)
	return sess
}

// cleanupService: testin yazdigi satirlari siler.
func cleanupService(t *testing.T, sess *gocql.Session, service string) {
	t.Cleanup(func() {
		iter := sess.Query(`SELECT DISTINCT service_name, metric_name FROM metrics`).Iter()
		var svc, metric string
		var toDelete []string
		for iter.Scan(&svc, &metric) {
			if svc == service {
				toDelete = append(toDelete, metric)
			}
		}
		_ = iter.Close()
		for _, m := range toDelete {
			_ = sess.Query(`DELETE FROM metrics WHERE service_name = ? AND metric_name = ?`,
				service, m).Exec()
		}
	})
}

// startCollector: collector'i arka planda calistirir, gRPC adresini dondurur.
func startCollector(t *testing.T, ctx context.Context, topic, group string) string {
	t.Helper()

	grpcPort := freePort(t)

	c, err := collector.NewCollector(&collector.Config{
		KafkaBrokers: []string{kafkaAddr},
		ScyllaAddr:   scyllaAddr,
		GRPCPort:     grpcPort,
		HealthAddr:   "127.0.0.1:" + freePort(t),
		Topic:        topic,
		GroupID:      group,
		// Bu testlerin trace ile isi yok; tuketiciyi kapatmak hem
		// gereksiz consumer group trafigini hem de varsayilan
		// pulse-traces topic'ine baglanmayi onler.
		DisableTraces: true,
		Debug:         false,
	})
	if err != nil {
		t.Fatalf("collector kurulamadi: %v", err)
	}

	done := make(chan error, 1)
	go func() { done <- c.Start(ctx) }()
	t.Cleanup(func() {
		select {
		case <-done:
		case <-time.After(20 * time.Second):
			t.Log("uyari: collector 20 sn icinde kapanmadi")
		}
	})

	addr := "127.0.0.1:" + grpcPort
	waitForTCP(t, addr, 30*time.Second)
	return addr
}

func waitForTCP(t *testing.T, addr string, limit time.Duration) {
	t.Helper()
	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, time.Second)
		if err == nil {
			_ = conn.Close()
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("%s %s icinde dinlemeye baslamadi", addr, limit)
}

// writeWithRetry: yeni yaratilan bir topic'e ilk yazma, lider secimi
// tamamlanmadan "Unknown Topic Or Partition" alabilir. Bu Kafka'nin normal
// davranisi; dogru cevap yeniden denemektir (agent'in ticker'i da bunu yapar).
func writeWithRetry(t *testing.T, w *kafka.Writer, msg kafka.Message) {
	t.Helper()

	deadline := time.Now().Add(60 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err := w.WriteMessages(ctx, msg)
		cancel()
		if err == nil {
			return
		}
		lastErr = err
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("Kafka'ya 60 sn boyunca yazilamadi: %v", lastErr)
}

func metricsClient(t *testing.T, addr string) pb.MetricsServiceClient {
	t.Helper()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("gRPC baglantisi kurulamadi: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return pb.NewMetricsServiceClient(conn)
}

// waitForSeries: sorgu en az minPoints nokta dondurene kadar bekler.
func waitForSeries(t *testing.T, client pb.MetricsServiceClient, service, metric string, minPoints int) *pb.MetricsQueryResponse {
	t.Helper()

	deadline := time.Now().Add(waitFor)
	var last *pb.MetricsQueryResponse

	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		resp, err := client.Query(ctx, &pb.MetricsQueryRequest{
			ServiceName: service,
			MetricName:  metric,
			StartTimeMs: time.Now().Add(-10 * time.Minute).UnixMilli(),
			EndTimeMs:   time.Now().Add(time.Minute).UnixMilli(),
		})
		cancel()

		if err == nil {
			last = resp
			total := 0
			for _, s := range resp.GetSeries() {
				total += len(s.GetPoints())
			}
			if total >= minPoints {
				return resp
			}
		}
		time.Sleep(time.Second)
	}

	t.Fatalf("%s/%s icin %s icinde %d nokta gelmedi (son yanit: %v)",
		service, metric, waitFor, minPoints, last)
	return nil
}

// --- testler -------------------------------------------------------------

// TestPipelineUctanUca: agent gercekten olcuyor, Kafka tasiyor, collector
// yaziyor ve gRPC sorgu servisi geri okuyabiliyor mu?
func TestPipelineUctanUca(t *testing.T) {
	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	topic := "pulse-itest-" + suffix
	service := "itest-" + suffix

	createTopic(t, topic)
	sess := scyllaSession(t)
	cleanupService(t, sess, service)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	grpcAddr := startCollector(t, ctx, topic, "itest-group-"+suffix)

	a, err := agent.NewAgent(&agent.Config{
		ServiceName:     service,
		InstanceID:      "inst-a",
		KafkaBrokers:    []string{kafkaAddr},
		CollectInterval: time.Second,
		HealthAddr:      "127.0.0.1:" + freePort(t),
		Topic:           topic,
	})
	if err != nil {
		t.Fatalf("agent kurulamadi: %v", err)
	}
	agentDone := make(chan error, 1)
	go func() { agentDone <- a.Start(ctx) }()

	client := metricsClient(t, grpcAddr)
	resp := waitForSeries(t, client, service, "process.memory.heap.alloc", 2)

	if len(resp.GetSeries()) != 1 {
		t.Fatalf("1 seri bekleniyordu, %d alindi", len(resp.GetSeries()))
	}
	s := resp.GetSeries()[0]

	// instance_id artik saklaniyor - eski surumde tamamen kayboluyordu.
	if s.GetInstanceId() != "inst-a" {
		t.Errorf("instance_id = %q, \"inst-a\" bekleniyordu", s.GetInstanceId())
	}
	// metrik tipi de saklaniyor.
	if s.GetType() != pb.MetricType_GAUGE {
		t.Errorf("type = %v, GAUGE bekleniyordu", s.GetType())
	}
	if s.GetTags()["unit"] != "bytes" {
		t.Errorf("tags[unit] = %q, \"bytes\" bekleniyordu", s.GetTags()["unit"])
	}

	// Noktalar eskiden yeniye sirali olmali (grafik boyle cizer).
	for i := 1; i < len(s.GetPoints()); i++ {
		if s.GetPoints()[i].GetTimestampMs() < s.GetPoints()[i-1].GetTimestampMs() {
			t.Fatalf("noktalar sirali degil: %d < %d",
				s.GetPoints()[i].GetTimestampMs(), s.GetPoints()[i-1].GetTimestampMs())
		}
	}

	// COUNTER tipi de dogru saklanmali.
	cResp := waitForSeries(t, client, service, "process.memory.gc.runs", 1)
	if got := cResp.GetSeries()[0].GetType(); got != pb.MetricType_COUNTER {
		t.Errorf("gc.runs tipi %v, COUNTER bekleniyordu", got)
	}

	// ListSeries bu servisi gormeli.
	lsCtx, lsCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer lsCancel()
	ls, err := client.ListSeries(lsCtx, &pb.ListSeriesRequest{ServiceName: service})
	if err != nil {
		t.Fatalf("ListSeries hatasi: %v", err)
	}
	if len(ls.GetSeries()) == 0 {
		t.Error("ListSeries bu servis icin bos dondu")
	}
	for _, ref := range ls.GetSeries() {
		if ref.GetServiceName() != service {
			t.Errorf("filtre calismadi: %q dondu", ref.GetServiceName())
		}
	}

	// Health RPC hazir demeli.
	hCtx, hCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer hCancel()
	h, err := client.Health(hCtx, &pb.HealthRequest{})
	if err != nil {
		t.Fatalf("Health hatasi: %v", err)
	}
	if !h.GetReady() {
		t.Errorf("collector hazir degil: %s", h.GetDetail())
	}

	// Duzgun kapanis: ctx iptal edilince agent temiz donmeli.
	cancel()
	select {
	case err := <-agentDone:
		if err != nil {
			t.Errorf("agent temiz kapanmadi: %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Error("agent 20 sn icinde kapanmadi")
	}
}

// TestIkiInstanceAyniZamanDamgasi: duzeltilen veri kaybi hatasinin
// gerilemedigini dogrular.
//
// Eski sema PRIMARY KEY ((service_name, metric_name), timestamp) idi ve
// instance_id hic saklanmiyordu; ayni servisin iki kopyasi ayni milisaniyede
// olcum gonderdiginde biri digerinin uzerine yaziyordu. Yeni semada
// instance_id clustering key'in parcasi, yani iki ayri satir olusmali.
func TestIkiInstanceAyniZamanDamgasi(t *testing.T) {
	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	topic := "pulse-itest-dup-" + suffix
	service := "itest-dup-" + suffix

	createTopic(t, topic)
	sess := scyllaSession(t)
	cleanupService(t, sess, service)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	grpcAddr := startCollector(t, ctx, topic, "itest-dup-group-"+suffix)

	// Ayni zaman damgasi, farkli instance.
	ts := time.Now().UnixMilli()
	w := &kafka.Writer{
		Addr:         kafka.TCP(kafkaAddr),
		Topic:        topic,
		RequiredAcks: kafka.RequireAll,
	}
	defer func() { _ = w.Close() }()

	for _, inst := range []string{"inst-a", "inst-b"} {
		payload := &pb.MetricsPayload{
			ServiceName: service,
			InstanceId:  inst,
			Timestamp:   timestamppb.Now(),
			Metrics: []*pb.Metric{{
				Name:            "test.collision",
				Type:            pb.MetricType_GAUGE,
				Value:           map[string]float64{"inst-a": 1, "inst-b": 2}[inst],
				TimestampMillis: ts, // ayni milisaniye
			}},
		}
		data, err := proto.Marshal(payload)
		if err != nil {
			t.Fatalf("marshal hatasi: %v", err)
		}
		writeWithRetry(t, w, kafka.Message{Key: []byte(service), Value: data})
	}

	client := metricsClient(t, grpcAddr)
	resp := waitForSeries(t, client, service, "test.collision", 2)

	if len(resp.GetSeries()) != 2 {
		t.Fatalf("2 instance icin 2 seri bekleniyordu, %d alindi -- "+
			"instance'lar birbirinin uzerine yaziyor olabilir", len(resp.GetSeries()))
	}

	got := map[string]float64{}
	for _, s := range resp.GetSeries() {
		if len(s.GetPoints()) != 1 {
			t.Errorf("%s icin 1 nokta bekleniyordu, %d alindi",
				s.GetInstanceId(), len(s.GetPoints()))
			continue
		}
		got[s.GetInstanceId()] = s.GetPoints()[0].GetValue()
	}

	if got["inst-a"] != 1 {
		t.Errorf("inst-a degeri %v, 1 bekleniyordu", got["inst-a"])
	}
	if got["inst-b"] != 2 {
		t.Errorf("inst-b degeri %v, 2 bekleniyordu", got["inst-b"])
	}

	// instance filtresi tek seri dondurmeli.
	fCtx, fCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer fCancel()
	filtered, err := client.Query(fCtx, &pb.MetricsQueryRequest{
		ServiceName: service,
		MetricName:  "test.collision",
		InstanceId:  "inst-b",
		StartTimeMs: ts - 60000,
		EndTimeMs:   ts + 60000,
	})
	if err != nil {
		t.Fatalf("filtreli sorgu hatasi: %v", err)
	}
	if len(filtered.GetSeries()) != 1 || filtered.GetSeries()[0].GetInstanceId() != "inst-b" {
		t.Errorf("instance filtresi calismadi: %+v", filtered.GetSeries())
	}
}

// TestSorguDogrulama: eksik parametreler anlamli hata dondurmeli.
func TestSorguDogrulama(t *testing.T) {
	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	topic := "pulse-itest-val-" + suffix
	createTopic(t, topic)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	grpcAddr := startCollector(t, ctx, topic, "itest-val-group-"+suffix)
	client := metricsClient(t, grpcAddr)

	cases := []struct {
		name string
		req  *pb.MetricsQueryRequest
	}{
		{"servis adi yok", &pb.MetricsQueryRequest{MetricName: "x"}},
		{"metrik adi yok", &pb.MetricsQueryRequest{ServiceName: "x"}},
		{"ters zaman araligi", &pb.MetricsQueryRequest{
			ServiceName: "x", MetricName: "y",
			StartTimeMs: 2000, EndTimeMs: 1000,
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			qCtx, qCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer qCancel()
			if _, err := client.Query(qCtx, tc.req); err == nil {
				t.Fatal("hata bekleniyordu, nil dondu")
			}
		})
	}
}
