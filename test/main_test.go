//go:build integration

package integration

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/nisah/pulse-metrics/internal/collector"
)

// TestMain: entegrasyon paketinin ortak hazirligi.
//
// # NEDEN GEREKLI OLDUGU NASIL ANLASILDI
//
// Faz 5'te CI kurulunca entegrasyon testleri ilk kosuda dustu:
//
//	Keyspace 'pulse' does not exist
//
// Sebep, testlerin cogunun scyllaSession() ile dogrudan "pulse"
// keyspace'ine baglanmasi. Temiz bir veritabaninda o keyspace henuz
// yoktur - ilk collector acilana kadar yaratilmaz. Gelistirici
// makinesinde keyspace aylardir var oldugu icin bu varsayim hic
// gorunmemisti.
//
// Bu, CI'in tam olarak ne ise yaradiginin ornegi: testler dogruydu ama
// TEMIZ bir ortamda calismiyorlardi ve bunu ancak temiz bir ortam
// gosterebilirdi.
func TestMain(m *testing.M) {
	if err := prepareSchema(); err != nil {
		fmt.Fprintf(os.Stderr,
			"entegrasyon testleri hazirlanamadi: %v\n"+
				"(docker compose up -d calistirdin mi?)\n", err)
		os.Exit(1)
	}
	os.Exit(m.Run())
}

// prepareSchema: keyspace ve tum tablolari bir kez yaratir.
//
// Semayi elle yazmak yerine collector'i bir kez acip hemen kapatiyoruz:
// NewCollector zaten acilista butun semayi kuruyor. Boylece sema tanimi
// tek yerde kaliyor ve test ile urun kodu asla ayrisamiyor.
//
// Dongu ayrica altyapinin hazir olmasini bekliyor - ScyllaDB acilisi
// yarim dakikayi bulabiliyor.
func prepareSchema() error {
	deadline := time.Now().Add(3 * time.Minute)
	var lastErr error

	for time.Now().Before(deadline) {
		c, err := collector.NewCollector(&collector.Config{
			KafkaBrokers: []string{kafkaAddr},
			ScyllaAddr:   scyllaAddr,
			// Bu collector hicbir sey tuketmeyecek; yalnizca semayi
			// kurmak icin aciliyor.
			GRPCPort:      "0",
			HealthAddr:    "127.0.0.1:0",
			InstanceID:    "itest-bootstrap",
			DisableTraces: true,
			DisableLogs:   true,
			DisableAlerts: true,
		})
		if err == nil {
			return c.Close()
		}
		lastErr = err
		time.Sleep(3 * time.Second)
	}
	return lastErr
}
