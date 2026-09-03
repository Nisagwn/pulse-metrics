package otlp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/segmentio/kafka-go"
	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/nisah/pulse-metrics/internal/obs"
)

// maxBodyBytes: OTLP/HTTP govde siniri.
//
// Sinirsiz okumak, tek bir istegin sureci bellek yetersizliginden
// oldurmesine izin vermek demektir. Gozlemlenebilirlik verisini gonderen
// uygulama senin kontrolunde olmayabilir.
const maxBodyBytes = 16 << 20 // 16 MiB

// Receiver: OTLP verisini alip Kafka'ya yazar.
//
// # NEDEN AYRI BIR SUREC?
//
// Bu alici collector'in icine de konabilirdi ama mimari olarak yanlis
// olurdu. PulseMetrics'te ingest siniri KAFKA: yazabilen her sey gecerli
// bir kaynaktir. Collector'in isi Kafka'dan okuyup ScyllaDB'ye yazmak;
// alicinin isi HTTP/gRPC'den okuyup Kafka'ya yazmak. Ikisi farkli
// olceklenir - alici istemci sayisiyla, collector veri hacmiyle.
//
// Bu ayrim ayni zamanda soyle bir sey soyluyor: OTLP alicisi ozel bir
// bilesen degil, sadece bir uretici. Kendi SDK'miz de oyle.
type Receiver struct {
	logger *zap.Logger

	traces  *kafka.Writer
	metrics *kafka.Writer
	logs    *kafka.Writer
}

// Config: alici ayarlari.
type Config struct {
	KafkaBrokers []string
	TracesTopic  string
	MetricsTopic string
	LogsTopic    string
	Logger       *zap.Logger
}

// NewReceiver: alici olusturur.
func NewReceiver(cfg Config) *Receiver {
	writer := func(topic string) *kafka.Writer {
		return &kafka.Writer{
			Addr:        kafka.TCP(cfg.KafkaBrokers...),
			Topic:       topic,
			Compression: kafka.Snappy,
			// RequireOne: alici, yazma basarisiz olursa istemciye HATA
			// donuyor ve OTel SDK'lari kendiliginden yeniden deniyor.
			// Yani veriyi burada tamponlamaya gerek yok - tamponlasak,
			// alici cokunce tampon da giderdi. Yeniden denemeyi
			// istemciye birakmak daha saglam.
			RequiredAcks:    kafka.RequireOne,
			WriteBackoffMin: 100 * time.Millisecond,
			WriteBackoffMax: time.Second,
		}
	}
	return &Receiver{
		logger:  cfg.Logger,
		traces:  writer(cfg.TracesTopic),
		metrics: writer(cfg.MetricsTopic),
		logs:    writer(cfg.LogsTopic),
	}
}

// Close: Kafka yazicilarini kapatir.
func (r *Receiver) Close() error {
	var firstErr error
	for _, w := range []*kafka.Writer{r.traces, r.metrics, r.logs} {
		if err := w.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// publish: yukleri Kafka'ya yazar. Anahtar servis adi: ayni servisin
// verisi ayni partition'a gider ve sirasi korunur.
func (r *Receiver) publish(ctx context.Context, w *kafka.Writer, signal string, payloads []proto.Message, keys []string) error {
	if len(payloads) == 0 {
		return nil
	}
	msgs := make([]kafka.Message, 0, len(payloads))
	for i, p := range payloads {
		data, err := proto.Marshal(p)
		if err != nil {
			return fmt.Errorf("yuk kodlanamadi: %w", err)
		}
		msgs = append(msgs, kafka.Message{Key: []byte(keys[i]), Value: data})
	}

	// ISTEK ICINDE kisa bir yeniden deneme.
	//
	// Ilk surum tek deneme yapip basarisizlikta 503 donuyordu; mantik
	// "istemci zaten yeniden dener" idi. Dogru ama yetersiz: yeni
	// yaratilmis bir topic'in broker'lara yayilmasi gibi saniyelik
	// gecici durumlarda her istegi reddetmek gereksiz gurultu uretiyor.
	//
	// Buradaki denemeler istegin OMRU icinde kaliyor - istemci hala
	// bekliyor, hicbir sey tamponlanmiyor. Yani surec cokerse kaybolacak
	// bir veri yok; sadece gecici bir sorunu istemciye tasimadan once
	// birkac saniye sabrediyoruz. Kalici bir sorunda yine 503 doner ve
	// OTel SDK'lari ustel geri cekilmeyle tekrar dener.
	// Butce: en fazla retryBudget kadar ugras, sonra istemciye birak.
	// Ilk denemem 3 deneme / ~1.5 saniyeydi ve yetmedi - yeni yaratilmis
	// bir topic'in yayilmasi bundan uzun surebiliyor. Faz 3'te trace
	// gondericisinde ayni hatayi yapip ayni sekilde ogrenmistim.
	//
	// Ust sinir istemcinin sabrina gore secildi: OTel SDK'larinin
	// varsayilan istek zaman asimi 10 saniye. 8 saniyede pes etmek,
	// istemcinin zaman asimina ugramasindan once ona duzgun bir 503
	// donebilmemizi sagliyor - zaman asimi, hata mesaji tasimaz.
	const retryBudget = 8 * time.Second

	deadline := time.Now().Add(retryBudget)
	backoff := 300 * time.Millisecond

	var lastErr error
	for attempt := 0; ; attempt++ {
		if attempt > 0 {
			if time.Now().Add(backoff).After(deadline) {
				break
			}
			select {
			case <-time.After(backoff):
				backoff *= 2
			case <-ctx.Done():
				obs.OTLPRejected.WithLabelValues(signal, "kafka").Add(float64(len(msgs)))
				return ctx.Err()
			}
		}

		writeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		lastErr = w.WriteMessages(writeCtx, msgs...)
		cancel()
		if lastErr == nil {
			return nil
		}
		if time.Now().After(deadline) {
			break
		}
	}

	obs.OTLPRejected.WithLabelValues(signal, "kafka").Add(float64(len(msgs)))
	return lastErr
}

// --- gRPC servisleri ---------------------------------------------------------
//
// Uc servisin de metodu "Export" oldugu icin uc ayri tip gerekiyor;
// tek bir tip ucunu birden uygulayamaz.

type traceService struct {
	coltracepb.UnimplementedTraceServiceServer
	r *Receiver
}

type metricService struct {
	colmetricspb.UnimplementedMetricsServiceServer
	r *Receiver
}

type logService struct {
	collogspb.UnimplementedLogsServiceServer
	r *Receiver
}

// Register: gRPC sunucusuna uc OTLP servisini de baglar.
func (r *Receiver) Register(srv *grpc.Server) {
	coltracepb.RegisterTraceServiceServer(srv, &traceService{r: r})
	colmetricspb.RegisterMetricsServiceServer(srv, &metricService{r: r})
	collogspb.RegisterLogsServiceServer(srv, &logService{r: r})
}

func (s *traceService) Export(ctx context.Context, req *coltracepb.ExportTraceServiceRequest) (*coltracepb.ExportTraceServiceResponse, error) {
	rejected, err := s.r.handleTraces(ctx, req)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "span'ler kuyruga yazilamadi: %v", err)
	}
	resp := &coltracepb.ExportTraceServiceResponse{}
	if rejected > 0 {
		resp.PartialSuccess = &coltracepb.ExportTracePartialSuccess{
			RejectedSpans: rejected,
			ErrorMessage:  "gecersiz trace_id/span_id ya da eksik zaman damgasi",
		}
	}
	return resp, nil
}

func (s *metricService) Export(ctx context.Context, req *colmetricspb.ExportMetricsServiceRequest) (*colmetricspb.ExportMetricsServiceResponse, error) {
	rejected, err := s.r.handleMetrics(ctx, req)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "metrikler kuyruga yazilamadi: %v", err)
	}
	resp := &colmetricspb.ExportMetricsServiceResponse{}
	if rejected > 0 {
		resp.PartialSuccess = &colmetricspb.ExportMetricsPartialSuccess{
			RejectedDataPoints: rejected,
			ErrorMessage:       "desteklenmeyen metrik turu (ustel histogram) ya da gecersiz deger",
		}
	}
	return resp, nil
}

func (s *logService) Export(ctx context.Context, req *collogspb.ExportLogsServiceRequest) (*collogspb.ExportLogsServiceResponse, error) {
	rejected, err := s.r.handleLogs(ctx, req)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "loglar kuyruga yazilamadi: %v", err)
	}
	resp := &collogspb.ExportLogsServiceResponse{}
	if rejected > 0 {
		resp.PartialSuccess = &collogspb.ExportLogsPartialSuccess{
			RejectedLogRecords: rejected,
			ErrorMessage:       "zaman damgasi olmayan kayit",
		}
	}
	return resp, nil
}

// --- ortak isleme ------------------------------------------------------------

func (r *Receiver) handleTraces(ctx context.Context, req *coltracepb.ExportTraceServiceRequest) (int64, error) {
	payloads, rejected := ConvertTraces(req.GetResourceSpans())

	msgs := make([]proto.Message, 0, len(payloads))
	keys := make([]string, 0, len(payloads))
	var accepted int
	for _, p := range payloads {
		msgs = append(msgs, p)
		keys = append(keys, p.ServiceName)
		accepted += len(p.Spans)
	}

	if err := r.publish(ctx, r.traces, obs.SignalTraces, msgs, keys); err != nil {
		return rejected, err
	}
	obs.OTLPReceived.WithLabelValues(obs.SignalTraces).Add(float64(accepted))
	if rejected > 0 {
		obs.OTLPRejected.WithLabelValues(obs.SignalTraces, "convert").Add(float64(rejected))
	}
	return rejected, nil
}

func (r *Receiver) handleMetrics(ctx context.Context, req *colmetricspb.ExportMetricsServiceRequest) (int64, error) {
	payloads, rejected := ConvertMetrics(req.GetResourceMetrics())

	msgs := make([]proto.Message, 0, len(payloads))
	keys := make([]string, 0, len(payloads))
	var accepted int
	for _, p := range payloads {
		msgs = append(msgs, p)
		keys = append(keys, p.ServiceName)
		accepted += len(p.Metrics)
	}

	if err := r.publish(ctx, r.metrics, obs.SignalMetrics, msgs, keys); err != nil {
		return rejected, err
	}
	obs.OTLPReceived.WithLabelValues(obs.SignalMetrics).Add(float64(accepted))
	if rejected > 0 {
		obs.OTLPRejected.WithLabelValues(obs.SignalMetrics, "convert").Add(float64(rejected))
	}
	return rejected, nil
}

func (r *Receiver) handleLogs(ctx context.Context, req *collogspb.ExportLogsServiceRequest) (int64, error) {
	payloads, rejected := ConvertLogs(req.GetResourceLogs())

	msgs := make([]proto.Message, 0, len(payloads))
	keys := make([]string, 0, len(payloads))
	var accepted int
	for _, p := range payloads {
		msgs = append(msgs, p)
		keys = append(keys, p.ServiceName)
		accepted += len(p.Logs)
	}

	if err := r.publish(ctx, r.logs, obs.SignalLogs, msgs, keys); err != nil {
		return rejected, err
	}
	obs.OTLPReceived.WithLabelValues(obs.SignalLogs).Add(float64(accepted))
	if rejected > 0 {
		obs.OTLPRejected.WithLabelValues(obs.SignalLogs, "convert").Add(float64(rejected))
	}
	return rejected, nil
}

// --- OTLP/HTTP ---------------------------------------------------------------

// Handler: OTLP/HTTP uclarini bir mux'a baglar.
//
// gRPC varken HTTP neden gerekli? Cunku her ortamda gRPC kullanilamiyor
// (tarayici, bazi sunucusuz ortamlar, katı ag politikalari) ve OTel
// SDK'larinin cogunun varsayilani HTTP. Ayrica JSON destegi sayesinde
// protokolu duz curl ile denemek mumkun - ki bir seyin gercekten
// calistigina inanmanin en hizli yolu budur.
func (r *Receiver) Handler(mux *http.ServeMux) {
	mux.HandleFunc("/v1/traces", r.httpHandler(
		func() proto.Message { return &coltracepb.ExportTraceServiceRequest{} },
		func(ctx context.Context, m proto.Message) (proto.Message, error) {
			req := m.(*coltracepb.ExportTraceServiceRequest)
			rejected, err := r.handleTraces(ctx, req)
			if err != nil {
				return nil, err
			}
			resp := &coltracepb.ExportTraceServiceResponse{}
			if rejected > 0 {
				resp.PartialSuccess = &coltracepb.ExportTracePartialSuccess{RejectedSpans: rejected}
			}
			return resp, nil
		}))

	mux.HandleFunc("/v1/metrics", r.httpHandler(
		func() proto.Message { return &colmetricspb.ExportMetricsServiceRequest{} },
		func(ctx context.Context, m proto.Message) (proto.Message, error) {
			req := m.(*colmetricspb.ExportMetricsServiceRequest)
			rejected, err := r.handleMetrics(ctx, req)
			if err != nil {
				return nil, err
			}
			resp := &colmetricspb.ExportMetricsServiceResponse{}
			if rejected > 0 {
				resp.PartialSuccess = &colmetricspb.ExportMetricsPartialSuccess{RejectedDataPoints: rejected}
			}
			return resp, nil
		}))

	mux.HandleFunc("/v1/logs", r.httpHandler(
		func() proto.Message { return &collogspb.ExportLogsServiceRequest{} },
		func(ctx context.Context, m proto.Message) (proto.Message, error) {
			req := m.(*collogspb.ExportLogsServiceRequest)
			rejected, err := r.handleLogs(ctx, req)
			if err != nil {
				return nil, err
			}
			resp := &collogspb.ExportLogsServiceResponse{}
			if rejected > 0 {
				resp.PartialSuccess = &collogspb.ExportLogsPartialSuccess{RejectedLogRecords: rejected}
			}
			return resp, nil
		}))
}

// httpHandler: OTLP/HTTP icin ortak govde - kod cozme, isleme, kodlama.
func (r *Receiver) httpHandler(
	newReq func() proto.Message,
	handle func(context.Context, proto.Message) (proto.Message, error),
) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodPost {
			http.Error(w, "yalnizca POST", http.StatusMethodNotAllowed)
			return
		}

		body, err := io.ReadAll(io.LimitReader(req.Body, maxBodyBytes+1))
		if err != nil {
			http.Error(w, "govde okunamadi", http.StatusBadRequest)
			return
		}
		if len(body) > maxBodyBytes {
			http.Error(w, "govde cok buyuk", http.StatusRequestEntityTooLarge)
			return
		}

		// OTLP/HTTP iki kodlama tanimlar. Content-Type hangisi oldugunu
		// soyler; JSON varsayilan degil, acikca istenmeli.
		isJSON := isJSONContentType(req.Header.Get("Content-Type"))

		msg := newReq()
		if isJSON {
			// OTLP/JSON kimlikleri ONALTILIK tasir, protojson ise
			// base64 bekler (bkz. json.go). Once cevir.
			body, err = normalizeJSONIDs(body)
			if err != nil {
				http.Error(w, "JSON isleneme: "+err.Error(), http.StatusBadRequest)
				return
			}
			// DiscardUnknown: OTLP semasi zamanla buyuyor. Bizim
			// bilmedigimiz yeni bir alan gonderen daha yeni bir SDK'yi
			// reddetmek yerine, anladigimiz kismi aliyoruz.
			if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(body, msg); err != nil {
				http.Error(w, "gecersiz JSON: "+err.Error(), http.StatusBadRequest)
				return
			}
		} else if err := proto.Unmarshal(body, msg); err != nil {
			http.Error(w, "gecersiz protobuf: "+err.Error(), http.StatusBadRequest)
			return
		}

		resp, err := handle(req.Context(), msg)
		if err != nil {
			// 503: gecici bir sorun, istemci yeniden denemeli.
			// OTel SDK'lari bu kodu gorunce ustel geri cekilmeyle
			// tekrar dener - tam istedigimiz davranis.
			r.logger.Error("OTLP istegi islenemedi", zap.Error(err))
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}

		var out []byte
		if isJSON {
			out, err = protojson.Marshal(resp)
			w.Header().Set("Content-Type", "application/json")
		} else {
			out, err = proto.Marshal(resp)
			w.Header().Set("Content-Type", "application/x-protobuf")
		}
		if err != nil {
			http.Error(w, "yanit kodlanamadi", http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
		if _, err := w.Write(out); err != nil && !errors.Is(err, http.ErrHandlerTimeout) {
			r.logger.Debug("OTLP yaniti yazilamadi", zap.Error(err))
		}
	}
}

func isJSONContentType(ct string) bool {
	for i := 0; i < len(ct); i++ {
		if ct[i] == ';' {
			ct = ct[:i]
			break
		}
	}
	switch trimSpace(ct) {
	case "application/json", "application/otlp+json":
		return true
	}
	return false
}

func trimSpace(s string) string {
	start := 0
	for start < len(s) && (s[start] == ' ' || s[start] == '\t') {
		start++
	}
	end := len(s)
	for end > start && (s[end-1] == ' ' || s[end-1] == '\t') {
		end--
	}
	return s[start:end]
}
