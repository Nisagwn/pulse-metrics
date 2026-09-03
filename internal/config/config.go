// Package config: ayarlari ortam degiskenlerinden okur ve dogrular.
//
// Neden ortam degiskeni?
//
// Faz 1-3'te her ayar bir CLI bayragiydi. Tek makinede elle calistirirken
// bu yeterli; bir konteyner orkestratorunde degil. Kubernetes bir Pod'a
// bayrak degil ortam degiskeni gecirir, ayni imajin dev/staging/prod'da
// farkli davranmasi da boyle saglanir. Bu yuzden oncelik sirasi:
//
//	CLI bayragi  >  ortam degiskeni  >  varsayilan
//
// Bayrak en ustte cunku elle mudahale her zaman kazanmali.
//
// Ikinci is: DOGRULAMA. Yanlis bir ayar, uygulama calisirken degil
// ACILISTA hata vermeli. "replication_factor=0" ile acilip ilk yazmada
// patlayan bir surec, hic acilmayan bir surecten daha kotudur; cunku
// orkestrator ilkini saglikli sanar.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Env: ortam degiskenini okur, yoksa varsayilani dondurur.
func Env(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

// EnvInt: sayisal ortam degiskeni. Cozulemezse varsayilan kullanilir.
func EnvInt(key string, def int) int {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

// EnvBool: "1", "true", "yes", "on" dogru sayilir.
func EnvBool(key string, def bool) bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv(key)))
	switch v {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	}
	return def
}

// EnvDuration: "30s", "5m" gibi Go sure bicimi.
func EnvDuration(key string, def time.Duration) time.Duration {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

// EnvList: virgulle ayrilmis liste ("a:1,b:2"). Bos ogeler atilir.
func EnvList(key string, def []string) []string {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	if len(out) == 0 {
		return def
	}
	return out
}

// --- ScyllaDB ---------------------------------------------------------------

// Scylla: veritabani baglanti ve replikasyon ayarlari.
type Scylla struct {
	// Hosts: birden fazla dugum verilebilir. Tek dugum vermek calisir
	// (surucu gossip ile digerlerini ogrenir) ama o TEK dugum acilista
	// kapaliysa baglanti hic kurulamaz. Uretimde en az uc adres verilir.
	Hosts []string

	Keyspace string

	// Consistency: bir yazma/okumanin kac replikadan onay bekledigi.
	//
	//	ONE           en hizli, en zayif garanti
	//	QUORUM        cogunluk; tek veri merkezinde dogru secim
	//	LOCAL_QUORUM  yerel veri merkezinde cogunluk; cok DC'de dogru secim
	//	              (QUORUM cok DC'de okyanus asiri onay bekler)
	Consistency string

	// ReplicationClass: SimpleStrategy tek veri merkezi/gelistirme icindir.
	// Uretimde NetworkTopologyStrategy kullanilir; hangi DC'de kac kopya
	// olacagini rack farkindaligiyla belirler.
	ReplicationClass  string
	ReplicationFactor int
	// LocalDC: NetworkTopologyStrategy ve LOCAL_QUORUM icin gerekli.
	LocalDC string

	Timeout        time.Duration
	ConnectTimeout time.Duration
	// NumConns: host basina baglanti sayisi. Tek baglanti uzerinden akan
	// yuksek hacimli yazma, surucu tarafinda siraya girer.
	NumConns int
}

// ScyllaFromEnv: PULSE_SCYLLA_* degiskenlerinden okur.
func ScyllaFromEnv(defaultAddr string) Scylla {
	return Scylla{
		Hosts:             EnvList("PULSE_SCYLLA_HOSTS", []string{defaultAddr}),
		Keyspace:          Env("PULSE_SCYLLA_KEYSPACE", "pulse"),
		Consistency:       Env("PULSE_SCYLLA_CONSISTENCY", "QUORUM"),
		ReplicationClass:  Env("PULSE_SCYLLA_REPLICATION_CLASS", "SimpleStrategy"),
		ReplicationFactor: EnvInt("PULSE_SCYLLA_REPLICATION_FACTOR", 1),
		LocalDC:           Env("PULSE_SCYLLA_LOCAL_DC", ""),
		Timeout:           EnvDuration("PULSE_SCYLLA_TIMEOUT", 10*time.Second),
		ConnectTimeout:    EnvDuration("PULSE_SCYLLA_CONNECT_TIMEOUT", 15*time.Second),
		NumConns:          EnvInt("PULSE_SCYLLA_NUM_CONNS", 2),
	}
}

// Validate: acilista cagrilir. Yanlis ayarla acilmaktansa hic acilmamak
// dogru davranis.
func (s Scylla) Validate() error {
	if len(s.Hosts) == 0 {
		return fmt.Errorf("scylla: en az bir host gerekli (PULSE_SCYLLA_HOSTS)")
	}
	if s.Keyspace == "" {
		return fmt.Errorf("scylla: keyspace bos olamaz")
	}
	if s.ReplicationFactor < 1 {
		return fmt.Errorf("scylla: replication_factor >= 1 olmali, %d verildi", s.ReplicationFactor)
	}

	switch strings.ToUpper(s.Consistency) {
	case "ONE", "TWO", "THREE", "QUORUM", "ALL", "LOCAL_QUORUM", "EACH_QUORUM", "LOCAL_ONE", "ANY":
	default:
		return fmt.Errorf("scylla: bilinmeyen consistency %q", s.Consistency)
	}

	switch s.ReplicationClass {
	case "SimpleStrategy":
		// SimpleStrategy rack/DC farkindaligi olmadan kopya yerlestirir.
		// Cok DC'li bir kumede bu, iki kopyanin ayni DC'ye dusmesine ve
		// o DC kaybedildiginde verinin gitmesine yol acabilir.
		if s.LocalDC != "" {
			return fmt.Errorf("scylla: local_dc verildiyse replication_class NetworkTopologyStrategy olmali")
		}
	case "NetworkTopologyStrategy":
		if s.LocalDC == "" {
			return fmt.Errorf("scylla: NetworkTopologyStrategy icin PULSE_SCYLLA_LOCAL_DC zorunlu")
		}
	default:
		return fmt.Errorf("scylla: bilinmeyen replication_class %q (SimpleStrategy, NetworkTopologyStrategy)", s.ReplicationClass)
	}

	if strings.EqualFold(s.Consistency, "LOCAL_QUORUM") && s.LocalDC == "" {
		return fmt.Errorf("scylla: LOCAL_QUORUM icin PULSE_SCYLLA_LOCAL_DC zorunlu")
	}

	// Tek kopyayla QUORUM aslinda ONE demektir; gelistirmede normal ama
	// uretimde tek dugumlu bir kumeye yazdiginin farkinda olunmali.
	if s.ReplicationFactor == 1 && len(s.Hosts) > 1 {
		return fmt.Errorf("scylla: %d host verildi ama replication_factor 1; dugum kaybinda veri kaybi olur", len(s.Hosts))
	}
	return nil
}

// ReplicationCQL: CREATE KEYSPACE icin replication haritasi.
func (s Scylla) ReplicationCQL() string {
	if s.ReplicationClass == "NetworkTopologyStrategy" {
		return fmt.Sprintf("{'class': 'NetworkTopologyStrategy', '%s': %d}",
			s.LocalDC, s.ReplicationFactor)
	}
	return fmt.Sprintf("{'class': 'SimpleStrategy', 'replication_factor': %d}",
		s.ReplicationFactor)
}

// --- kimlik -----------------------------------------------------------------

// InstanceID: bu surecin kumedeki adi.
//
// Faz 4'te sart oldu: iki collector ayni anda calistiginda "bu alarmi kim
// gonderdi?" sorusunun cevabi olmali. Kubernetes Pod adini otomatik
// verir; disarida hostname yeterli.
func InstanceID(explicit string) string {
	if explicit != "" {
		return explicit
	}
	if v := Env("PULSE_INSTANCE_ID", ""); v != "" {
		return v
	}
	if v := Env("HOSTNAME", ""); v != "" {
		return v
	}
	if host, err := os.Hostname(); err == nil && host != "" {
		return host
	}
	return "unknown"
}
