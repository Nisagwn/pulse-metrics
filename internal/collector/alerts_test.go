package collector

import (
	"math"
	"testing"
)

func TestParseCondition(t *testing.T) {
	cases := []struct {
		in   string
		agg  string
		op   string
		want float64
	}{
		{"p95 > 500", "p95", ">", 500},
		{"avg >= 10.5", "avg", ">=", 10.5},
		{"min < 1", "min", "<", 1},
		{"zscore > 3", "zscore", ">", 3},
		{"  max   <=   0.5  ", "max", "<=", 0.5},
		{"AVG > 100", "avg", ">", 100}, // buyuk harf kabul edilmeli
	}

	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			c, err := ParseCondition(tc.in)
			if err != nil {
				t.Fatalf("beklenmeyen hata: %v", err)
			}
			if c.Aggregation != tc.agg || c.Operator != tc.op || c.Threshold != tc.want {
				t.Errorf("cozulen = %+v, beklenen {%s %s %g}", c, tc.agg, tc.op, tc.want)
			}
		})
	}
}

func TestParseConditionGecersiz(t *testing.T) {
	cases := map[string]string{
		"bos":                 "",
		"eksik parca":         "p95 > ",
		"fazla parca":         "p95 > 500 ve avg > 1",
		"bilinmeyen toplama":  "medyan > 5",
		"bilinmeyen operator": "avg == 5",
		"sayi degil":          "avg > cok",
		"sadece sayi":         "500",
		// Kullanicidan gelen metni degerlendirmedigimizin kaniti:
		"kod enjeksiyonu": "avg > 1; DROP TABLE metrics",
	}
	for name, in := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := ParseCondition(in); err == nil {
				t.Errorf("hata bekleniyordu: %q", in)
			}
		})
	}
}

func TestConditionBreached(t *testing.T) {
	cases := []struct {
		cond  string
		value float64
		want  bool
	}{
		{"avg > 100", 101, true},
		{"avg > 100", 100, false},
		{"avg >= 100", 100, true},
		{"avg < 10", 9, true},
		{"avg < 10", 10, false},
		{"avg <= 10", 10, true},
		{"zscore > 3", 3.5, true},
		{"zscore > 3", -3.5, false}, // tek yonlu: sadece yukari sapma
	}
	for _, tc := range cases {
		c, err := ParseCondition(tc.cond)
		if err != nil {
			t.Fatal(err)
		}
		if got := c.Breached(tc.value); got != tc.want {
			t.Errorf("%q, deger %v -> %v, beklenen %v", tc.cond, tc.value, got, tc.want)
		}
	}
}

func TestMeanStdDev(t *testing.T) {
	// Bilinen degerler: 2,4,4,4,5,5,7,9
	// ortalama 5, ornek std sapmasi (n-1) = 2.13809...
	values := []float64{2, 4, 4, 4, 5, 5, 7, 9}
	mean, sd := meanStdDev(values)

	if mean != 5 {
		t.Errorf("ortalama = %v, beklenen 5", mean)
	}
	if math.Abs(sd-2.138089) > 0.0001 {
		t.Errorf("std sapma = %v, beklenen ~2.13809", sd)
	}
}

func TestMeanStdDevSinirlar(t *testing.T) {
	if m, s := meanStdDev(nil); m != 0 || s != 0 {
		t.Errorf("bos dilim (0,0) dondurmeli, alinan (%v,%v)", m, s)
	}
	// Tek eleman: n-1 = 0, sifira bolme olmamali.
	if m, s := meanStdDev([]float64{42}); m != 42 || s != 0 {
		t.Errorf("tek eleman (42,0) dondurmeli, alinan (%v,%v)", m, s)
	}
	// Tamamen sabit seri: sapma sifir.
	if _, s := meanStdDev([]float64{7, 7, 7, 7}); s != 0 {
		t.Errorf("sabit seride sapma 0 olmali, alinan %v", s)
	}
}

func TestAggregateFloats(t *testing.T) {
	values := []float64{10, 20, 30, 40, 50}
	cases := map[string]float64{
		"avg":   30,
		"sum":   150,
		"min":   10,
		"max":   50,
		"count": 5,
		"last":  50,
		"p50":   30,
		"p95":   50,
		"p99":   50,
	}
	for kind, want := range cases {
		t.Run(kind, func(t *testing.T) {
			// Her seferinde taze dilim: p50/p95 siralama yapiyor.
			got := aggregateFloats(kind, []float64{10, 20, 30, 40, 50})
			if got != want {
				t.Errorf("%s = %v, beklenen %v", kind, got, want)
			}
		})
	}
	// Girdi dilimi bozulmamali.
	_ = aggregateFloats("p95", values)
	if values[0] != 10 || values[4] != 50 {
		t.Errorf("aggregateFloats girdiyi degistirdi: %v", values)
	}
}

func TestAggregateFloatsBos(t *testing.T) {
	for _, kind := range []string{"avg", "sum", "min", "max", "p95"} {
		if got := aggregateFloats(kind, nil); got != 0 {
			t.Errorf("%s bos dilimde %v dondurdu, 0 bekleniyordu", kind, got)
		}
	}
}

func TestNormalize(t *testing.T) {
	cases := []struct{ in, want string }{
		{"kart reddedildi: yetersiz bakiye, tutar 187.28",
			"kart reddedildi: yetersiz bakiye, tutar <N>"},
		{"siparis ord-000123 olusturuldu",
			"siparis ord-<N> olusturuldu"},
		{"baglanti 192.168.1.44 adresinden geldi",
			"baglanti <IP> adresinden geldi"},
		{"trace 4bf92f3577b34da6a3ce929d0e0e4736 bulunamadi",
			"trace <HEX> bulunamadi"},
		{"kullanici 550e8400-e29b-41d4-a716-446655440000 silindi",
			"kullanici <UUID> silindi"},
		{`sorgu "SELECT * FROM x" basarisiz`,
			`sorgu "<STR>" basarisiz`},
		{"degisken parcasi olmayan mesaj",
			"degisken parcasi olmayan mesaj"},
	}
	for _, tc := range cases {
		if got := Normalize(tc.in); got != tc.want {
			t.Errorf("Normalize(%q)\n  = %q\n  beklenen %q", tc.in, got, tc.want)
		}
	}
}

// Ayni sablondan uretilmis farkli mesajlar tek kalipta birlesmeli -
// kalip tespitinin butun amaci bu.
func TestNormalizeAyniKalibaIndirger(t *testing.T) {
	a := Normalize("kart reddedildi: tutar 12.50")
	b := Normalize("kart reddedildi: tutar 987.00")
	c := Normalize("kart reddedildi: tutar 3")
	if a != b || b != c {
		t.Errorf("ayni kalip bekleniyordu:\n  %q\n  %q\n  %q", a, b, c)
	}
}

func TestParseSeverity(t *testing.T) {
	cases := map[string]string{
		"INFO":     "INFO",
		"warning":  "WARNING",
		"CRITICAL": "CRITICAL",
		"":         "WARNING", // bilinmeyen -> WARNING
		"saçma":    "WARNING",
	}
	for in, want := range cases {
		if got := parseSeverity(in).String(); got != want {
			t.Errorf("parseSeverity(%q) = %s, beklenen %s", in, got, want)
		}
	}
}
