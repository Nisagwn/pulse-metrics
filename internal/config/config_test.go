package config

import (
	"strings"
	"testing"
	"time"
)

func TestEnvYardimcilari(t *testing.T) {
	t.Setenv("PULSE_TEST_STR", "merhaba")
	t.Setenv("PULSE_TEST_INT", "42")
	t.Setenv("PULSE_TEST_BOOL", "yes")
	t.Setenv("PULSE_TEST_DUR", "90s")
	t.Setenv("PULSE_TEST_LIST", "a:1, b:2 ,, c:3")
	t.Setenv("PULSE_TEST_BOZUK", "sayi-degil")
	t.Setenv("PULSE_TEST_BOSLUK", "   ")

	if got := Env("PULSE_TEST_STR", "varsayilan"); got != "merhaba" {
		t.Errorf("Env = %q", got)
	}
	// Sadece bosluk iceren bir degisken "verilmemis" sayilmali; aksi halde
	// yanlislikla bos birakilan bir ayar varsayilani sessizce ezerdi.
	if got := Env("PULSE_TEST_BOSLUK", "varsayilan"); got != "varsayilan" {
		t.Errorf("bosluklu deger varsayilana dusmeliydi, %q alindi", got)
	}
	if got := Env("PULSE_TEST_YOK", "varsayilan"); got != "varsayilan" {
		t.Errorf("Env yok = %q", got)
	}
	if got := EnvInt("PULSE_TEST_INT", 7); got != 42 {
		t.Errorf("EnvInt = %d", got)
	}
	// Cozulemeyen deger varsayilana duser: kotu bir ayar yuzunden surec
	// acilmamasindansa varsayilanla acilmasi tercih edilir.
	if got := EnvInt("PULSE_TEST_BOZUK", 7); got != 7 {
		t.Errorf("bozuk EnvInt = %d, 7 bekleniyordu", got)
	}
	if got := EnvBool("PULSE_TEST_BOOL", false); !got {
		t.Error("EnvBool 'yes' true olmaliydi")
	}
	if got := EnvDuration("PULSE_TEST_DUR", time.Second); got != 90*time.Second {
		t.Errorf("EnvDuration = %v", got)
	}

	list := EnvList("PULSE_TEST_LIST", nil)
	want := []string{"a:1", "b:2", "c:3"}
	if len(list) != len(want) {
		t.Fatalf("EnvList = %v, %v bekleniyordu", list, want)
	}
	for i := range want {
		if list[i] != want[i] {
			t.Errorf("EnvList[%d] = %q, %q bekleniyordu", i, list[i], want[i])
		}
	}
}

func gecerliScylla() Scylla {
	return Scylla{
		Hosts:             []string{"localhost:9042"},
		Keyspace:          "pulse",
		Consistency:       "QUORUM",
		ReplicationClass:  "SimpleStrategy",
		ReplicationFactor: 1,
	}
}

func TestScyllaValidateGecerli(t *testing.T) {
	if err := gecerliScylla().Validate(); err != nil {
		t.Errorf("gecerli ayar reddedildi: %v", err)
	}

	prod := Scylla{
		Hosts:             []string{"n1:9042", "n2:9042", "n3:9042"},
		Keyspace:          "pulse",
		Consistency:       "LOCAL_QUORUM",
		ReplicationClass:  "NetworkTopologyStrategy",
		ReplicationFactor: 3,
		LocalDC:           "dc1",
	}
	if err := prod.Validate(); err != nil {
		t.Errorf("uretim ayari reddedildi: %v", err)
	}
}

// Yanlis ayar ACILISTA yakalanmali. Bu testin varlik sebebi: yanlis
// ayarla acilan bir surec orkestratore "saglikliyim" der ve sorun
// gorunmez kalir.
func TestScyllaValidateHatalar(t *testing.T) {
	cases := map[string]func(*Scylla){
		"host yok":              func(s *Scylla) { s.Hosts = nil },
		"keyspace bos":          func(s *Scylla) { s.Keyspace = "" },
		"rf sifir":              func(s *Scylla) { s.ReplicationFactor = 0 },
		"bilinmeyen tutarlilik": func(s *Scylla) { s.Consistency = "COK_GUCLU" },
		"bilinmeyen strateji":   func(s *Scylla) { s.ReplicationClass = "MagicStrategy" },
		"NTS ama DC yok": func(s *Scylla) {
			s.ReplicationClass = "NetworkTopologyStrategy"
		},
		"LOCAL_QUORUM ama DC yok": func(s *Scylla) {
			s.Consistency = "LOCAL_QUORUM"
		},
		"SimpleStrategy ama DC verilmis": func(s *Scylla) {
			s.LocalDC = "dc1"
		},
		// Uc dugum ama tek kopya: bir dugum kaybinda veri gider.
		// Sessizce kabul edilmesi tehlikeli olurdu.
		"cok dugum ama rf=1": func(s *Scylla) {
			s.Hosts = []string{"n1:9042", "n2:9042", "n3:9042"}
		},
	}

	for name, bozy := range cases {
		t.Run(name, func(t *testing.T) {
			s := gecerliScylla()
			bozy(&s)
			if err := s.Validate(); err == nil {
				t.Errorf("hata bekleniyordu: %+v", s)
			}
		})
	}
}

func TestReplicationCQL(t *testing.T) {
	simple := gecerliScylla().ReplicationCQL()
	if !strings.Contains(simple, "SimpleStrategy") || !strings.Contains(simple, "'replication_factor': 1") {
		t.Errorf("SimpleStrategy CQL = %s", simple)
	}

	nts := Scylla{
		ReplicationClass:  "NetworkTopologyStrategy",
		ReplicationFactor: 3,
		LocalDC:           "dc1",
	}.ReplicationCQL()
	if !strings.Contains(nts, "NetworkTopologyStrategy") || !strings.Contains(nts, "'dc1': 3") {
		t.Errorf("NetworkTopologyStrategy CQL = %s", nts)
	}
}

func TestInstanceID(t *testing.T) {
	// Acikca verilen her zaman kazanir.
	t.Setenv("PULSE_INSTANCE_ID", "ortamdan")
	if got := InstanceID("acikca"); got != "acikca" {
		t.Errorf("acik deger kazanmaliydi, %q alindi", got)
	}
	if got := InstanceID(""); got != "ortamdan" {
		t.Errorf("ortam degiskeni kullanilmaliydi, %q alindi", got)
	}

	// Hicbiri yoksa hostname'e duser; ne olursa olsun bos donmemeli,
	// cunku bu deger paylasilan alarm durumunda "sahip" olarak yaziliyor.
	t.Setenv("PULSE_INSTANCE_ID", "")
	t.Setenv("HOSTNAME", "")
	if got := InstanceID(""); got == "" {
		t.Error("InstanceID hicbir zaman bos donmemeli")
	}
}
