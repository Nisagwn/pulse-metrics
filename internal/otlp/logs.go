package otlp

import (
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/nisah/pulse-metrics/pkg/pulsepb"
)

// ConvertLogs: OTLP log kayitlarini PulseMetrics yuklerine cevirir.
func ConvertLogs(resourceLogs []*logspb.ResourceLogs) (payloads []*pb.LogsPayload, rejected int64) {
	for _, rl := range resourceLogs {
		if rl == nil {
			continue
		}

		resAttrs := Attributes(rl.GetResource().GetAttributes())
		service := resAttrs[AttrServiceName]
		if service == "" {
			service = unknownService
		}
		instance := resAttrs[AttrServiceInstanceID]
		if instance == "" {
			instance = resAttrs[AttrHostName]
		}
		if instance == "" {
			instance = "otlp"
		}
		delete(resAttrs, AttrServiceName)

		var records []*pb.LogRecord
		for _, sl := range rl.GetScopeLogs() {
			logger := sl.GetScope().GetName()
			for _, lr := range sl.GetLogRecords() {
				converted := convertLogRecord(lr, service, logger, resAttrs)
				if converted == nil {
					rejected++
					continue
				}
				records = append(records, converted)
			}
		}

		if len(records) == 0 {
			continue
		}
		payloads = append(payloads, &pb.LogsPayload{
			ServiceName: service,
			InstanceId:  instance,
			Logs:        records,
			Timestamp:   timestamppb.Now(),
		})
	}
	return payloads, rejected
}

func convertLogRecord(lr *logspb.LogRecord, service, logger string, resAttrs map[string]string) *pb.LogRecord {
	if lr == nil {
		return nil
	}

	// Zaman damgasi: OTLP iki alan tasir. TimeUnixNano olayin GERCEKTEN
	// olustugu an, ObservedTimeUnixNano ise toplayicinin onu gordugu an.
	// Ilki her zaman daha dogru ama istemci onu doldurmayabilir.
	ts := lr.GetTimeUnixNano()
	if ts == 0 {
		ts = lr.GetObservedTimeUnixNano()
	}
	if ts == 0 {
		// Zamansiz bir log kaydi zaman serisi tablosuna yazilamaz:
		// timestamp clustering key'in parcasi.
		return nil
	}

	out := &pb.LogRecord{
		TimestampMs: int64(ts / 1e6),
		Level:       convertSeverity(lr.GetSeverityNumber(), lr.GetSeverityText()),
		Message:     AnyValueString(lr.GetBody()),
		ServiceName: service,
		LoggerName:  logger,
		TraceId:     hexID(lr.GetTraceId(), 16),
		SpanId:      hexID(lr.GetSpanId(), 8),
		Attributes:  merge(Attributes(lr.GetAttributes()), resAttrs),
	}

	// OTel'in istisna sozlesmesi: yigin izi bir oznitelikte gelir.
	if out.Attributes != nil {
		if stack := out.Attributes["exception.stacktrace"]; stack != "" {
			out.StackTrace = stack
		}
	}
	return out
}

// convertSeverity: OTLP'nin 24 kademeli siddet olcegini PulseMetrics'in
// 5 seviyesine indirir.
//
// OTel her seviyeyi dorde boler (DEBUG, DEBUG2, DEBUG3, DEBUG4) ki
// kaynak sistemlerin ince ayrimlari korunabilsin. PulseMetrics'te bu
// ayrima gerek yok; SEVERITY_TEXT alani zaten ozgun etiketi tasiyor.
//
//	 1-4   TRACE  -> DEBUG   (ayri bir TRACE seviyemiz yok)
//	 5-8   DEBUG  -> DEBUG
//	 9-12  INFO   -> INFO
//	13-16  WARN   -> WARN
//	17-20  ERROR  -> ERROR
//	21-24  FATAL  -> FATAL
func convertSeverity(n logspb.SeverityNumber, text string) pb.LogLevel {
	switch {
	case n >= 21:
		return pb.LogLevel_LEVEL_FATAL
	case n >= 17:
		return pb.LogLevel_LEVEL_ERROR
	case n >= 13:
		return pb.LogLevel_LEVEL_WARN
	case n >= 9:
		return pb.LogLevel_LEVEL_INFO
	case n >= 1:
		return pb.LogLevel_LEVEL_DEBUG
	}

	// SeverityNumber verilmemis. Bazi kutuphaneler yalnizca metin
	// gonderiyor; onu okumayi denemek, her seyi UNSPECIFIED yapmaktan
	// iyi - aksi halde seviye filtresi bu kayitlari hic gostermezdi.
	if lvl := parseSeverityText(text); lvl != pb.LogLevel_LEVEL_UNSPECIFIED {
		return lvl
	}
	return pb.LogLevel_LEVEL_INFO
}

func parseSeverityText(text string) pb.LogLevel {
	switch normalizeLevel(text) {
	case "TRACE", "DEBUG":
		return pb.LogLevel_LEVEL_DEBUG
	case "INFO", "INFORMATION", "NOTICE":
		return pb.LogLevel_LEVEL_INFO
	case "WARN", "WARNING":
		return pb.LogLevel_LEVEL_WARN
	case "ERROR", "ERR", "SEVERE":
		return pb.LogLevel_LEVEL_ERROR
	case "FATAL", "CRITICAL", "PANIC":
		return pb.LogLevel_LEVEL_FATAL
	}
	return pb.LogLevel_LEVEL_UNSPECIFIED
}

func normalizeLevel(s string) string {
	out := make([]rune, 0, len(s))
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z':
			out = append(out, r-32)
		case r >= 'A' && r <= 'Z':
			out = append(out, r)
		}
	}
	return string(out)
}
