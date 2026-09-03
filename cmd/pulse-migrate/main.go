// pulse-migrate: metrics tablosunu Faz 4 semasina tasir.
//
// # SORUN
//
// Cassandra ve Scylla'da partition key DEGISTIRILEMEZ. ALTER TABLE ile
// kolon eklenebilir, tur genisletilebilir, TTL degistirilebilir - ama
// verinin hangi dugume dusecegini belirleyen anahtara dokunulamaz, cunku
// bu tum verinin kumede yeniden dagitilmasi demek. Yani:
//
//	((service_name, metric_name))  ->  ((service_name, metric_name, time_bucket))
//
// gecisi bir ALTER degil, bir TASIMA islemidir.
//
// BU ARACIN YAPTIGI (durdurmali goc)
//
//  1. metrics -> metrics_v4 (yeni sema, kova hesaplanarak, TTL korunarak)
//  2. DROP metrics; yeni semayla yeniden yarat
//  3. metrics_v4 -> metrics
//  4. metrics_v4'u sil (-keep-backup verilmediyse)
//
// Veri iki kez kopyalaniyor. Bunun sebebi tablo adinin sabit kalmasi
// gerekmesi: kod "metrics" yaziyor ve oyle kalmali. Ara tablo, DROP ile
// CREATE arasinda verinin duracagi yer.
//
// # BUYUK KUMELERDE BUNU KULLANMA
//
// Gercek bir uretim kumesinde dogru yontem kesintisiz gectir:
//
//  1. Collector'i her iki tabloya da yazacak sekilde dagit (dual-write)
//  2. Okumayi once eskiye, sonra yeniye cevir
//  3. TTL suresi kadar bekle (burada 30 gun) - eski tablo kendini bosaltir
//  4. Eski tabloyu sil
//
// Hicbir adimda kesinti yok ve kopyalama hic yapilmiyor; zamanin kendisi
// gocu tamamliyor. Bedeli 30 gun boyunca iki kat yazma. Bu proje o olcekte
// olmadigi icin burada durdurmali goc tercih edildi - ama secimin bilincli
// oldugunu bilmek onemli.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/gocql/gocql"

	"github.com/nisah/pulse-metrics/internal/collector"
)

const stagingTable = "metrics_v4"

func main() {
	var (
		addr       = flag.String("scylla", "localhost:9042", "ScyllaDB adresi (virgulle birden fazla)")
		keyspace   = flag.String("keyspace", "pulse", "Keyspace adi")
		confirm    = flag.Bool("confirm", false, "Gercekten calistir. Verilmezse sadece plan yazilir.")
		keepBackup = flag.Bool("keep-backup", false, "Ara tabloyu silme (geri donus icin)")
		pageSize   = flag.Int("page", 5000, "Okuma sayfa boyutu")
	)
	flag.Parse()

	hosts := strings.Split(*addr, ",")
	cluster := gocql.NewCluster(hosts...)
	cluster.Keyspace = *keyspace
	cluster.Consistency = gocql.Quorum
	cluster.Timeout = 30 * time.Second
	cluster.ConnectTimeout = 15 * time.Second

	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatalf("ScyllaDB'ye baglanilamadi: %v", err)
	}
	defer session.Close()

	pk, err := partitionKey(session, *keyspace, "metrics")
	if err != nil {
		log.Fatalf("sema okunamadi: %v", err)
	}

	switch {
	case len(pk) == 0:
		fmt.Println("metrics tablosu yok. Yapilacak bir sey yok; collector acilista yaratir.")
		return
	case contains(pk, "time_bucket"):
		fmt.Printf("metrics zaten Faz 4 semasinda (partition key: %s). Goc gerekmiyor.\n",
			strings.Join(pk, ", "))
		return
	}

	rows, err := countRows(session)
	if err != nil {
		log.Fatalf("satirlar sayilamadi: %v", err)
	}

	fmt.Println("PulseMetrics sema gocu: metrics -> Faz 4")
	fmt.Println()
	fmt.Printf("  keyspace        : %s\n", *keyspace)
	fmt.Printf("  mevcut PK       : ((%s), timestamp, instance_id)\n", strings.Join(pk, ", "))
	fmt.Printf("  hedef PK        : ((service_name, metric_name, time_bucket), timestamp, instance_id)\n")
	fmt.Printf("  tasinacak satir : %d\n", rows)
	fmt.Printf("  ara tablo       : %s\n", stagingTable)
	fmt.Println()

	if !*confirm {
		fmt.Println("Bu bir DENEME calismasi. Hicbir sey degistirilmedi.")
		fmt.Println("Gercekten calistirmak icin -confirm ekle.")
		fmt.Println()
		fmt.Println("UYARI: goc sirasinda metrics tablosu kisa sure YOK olur.")
		fmt.Println("Once collector'lari durdur; yoksa yazmalari hata verir.")
		return
	}

	started := time.Now()

	// 1) Ara tabloyu yeni semayla yarat ve doldur.
	if err := createStaging(session); err != nil {
		log.Fatalf("ara tablo yaratilamadi: %v", err)
	}
	n, err := copyRows(session, "metrics", stagingTable, *pageSize, true)
	if err != nil {
		log.Fatalf("ara tabloya kopyalanamadi: %v", err)
	}
	fmt.Printf("[1/3] %d satir %s tablosuna kopyalandi\n", n, stagingTable)

	// 2) Asil tabloyu yeni semayla yeniden yarat.
	//
	// Buradan sonrasi geri alinamaz. Ara tablo elimizde duruyor: bu adim
	// ile 3. adim arasinda bir sey ters giderse veri hala orada.
	if err := session.Query(`DROP TABLE metrics`).Exec(); err != nil {
		log.Fatalf("eski tablo silinemedi: %v", err)
	}
	if err := session.Query(collector.MetricsTableDDL).Exec(); err != nil {
		log.Fatalf("yeni tablo yaratilamadi (veri %s tablosunda duruyor): %v", stagingTable, err)
	}
	fmt.Println("[2/3] metrics tablosu yeni semayla yeniden yaratildi")

	// 3) Geri kopyala.
	n, err = copyRows(session, stagingTable, "metrics", *pageSize, false)
	if err != nil {
		log.Fatalf("geri kopyalanamadi (veri %s tablosunda duruyor): %v", stagingTable, err)
	}
	fmt.Printf("[3/3] %d satir metrics tablosuna geri yazildi\n", n)

	if *keepBackup {
		fmt.Printf("\nAra tablo %s korundu. Dogruladiktan sonra sil:\n", stagingTable)
		fmt.Printf("  DROP TABLE %s.%s;\n", *keyspace, stagingTable)
	} else {
		if err := session.Query(`DROP TABLE ` + stagingTable).Exec(); err != nil {
			fmt.Fprintf(os.Stderr, "uyari: ara tablo silinemedi: %v\n", err)
		}
	}

	fmt.Printf("\nGoc tamamlandi (%s). Collector'lari yeniden baslatabilirsin.\n",
		time.Since(started).Round(time.Millisecond))
}

// partitionKey: tablonun partition key kolonlari, sirasiyla.
func partitionKey(session *gocql.Session, keyspace, table string) ([]string, error) {
	iter := session.Query(`
		SELECT column_name, position FROM system_schema.columns
		WHERE keyspace_name = ? AND table_name = ? AND kind = 'partition_key'
		ALLOW FILTERING`, keyspace, table).Iter()

	type col struct {
		name string
		pos  int
	}
	var (
		name string
		pos  int
		cols []col
	)
	for iter.Scan(&name, &pos) {
		cols = append(cols, col{name, pos})
	}
	if err := iter.Close(); err != nil {
		return nil, err
	}

	// position, partition key icindeki sirayi verir.
	out := make([]string, len(cols))
	for _, c := range cols {
		if c.pos >= 0 && c.pos < len(out) {
			out[c.pos] = c.name
		}
	}
	return out, nil
}

func countRows(session *gocql.Session) (int64, error) {
	var n int64
	// Tam tarama. Kucuk/orta veri icin kabul edilebilir; goc zaten tum
	// tabloyu okuyacak, bu sadece kullaniciya ne kadar surecegini soyluyor.
	if err := session.Query(`SELECT COUNT(*) FROM metrics`).Scan(&n); err != nil {
		return 0, err
	}
	return n, nil
}

func createStaging(session *gocql.Session) error {
	ddl := strings.Replace(collector.MetricsTableDDL, "pulse.metrics", "pulse."+stagingTable, 1)
	return session.Query(ddl).Exec()
}

// copyRows: bir tablodan digerine satir kopyalar.
//
// computeBucket=true ise kaynakta time_bucket yoktur ve zaman damgasindan
// hesaplanir; false ise kaynak zaten yeni semadadir ve kova okunur.
//
// TTL'e dikkat: TTL(value) ile kalan sure okunup INSERT ... USING TTL ile
// yaziliyor. Bu yapilmasaydi tasinan her satirin sayaci sifirlanir, 29
// gunluk bir olcum 30 gun daha yasardi. Sessiz ama gercek bir hata olurdu:
// tablo kucuk kalmasi gerekirken buyumeye devam ederdi.
func copyRows(session *gocql.Session, from, to string, pageSize int, computeBucket bool) (int64, error) {
	cols := "service_name, metric_name, timestamp, instance_id, type, tags, labels, value, TTL(value)"
	if !computeBucket {
		cols = "service_name, metric_name, time_bucket, timestamp, instance_id, type, tags, labels, value, TTL(value)"
	}

	iter := session.Query(`SELECT ` + cols + ` FROM ` + from).PageSize(pageSize).Iter()

	insert := `INSERT INTO ` + to + ` (service_name, metric_name, time_bucket, timestamp,
	           instance_id, type, tags, labels, value) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`
	insertTTL := insert + ` USING TTL ?`

	var (
		svc, metric, bucket, instance, mtype string
		ts                                   int64
		tags, labels                         map[string]string
		value                                float64
		ttl                                  int
		copied                               int64
	)

	scan := func() bool {
		if computeBucket {
			return iter.Scan(&svc, &metric, &ts, &instance, &mtype, &tags, &labels, &value, &ttl)
		}
		return iter.Scan(&svc, &metric, &bucket, &ts, &instance, &mtype, &tags, &labels, &value, &ttl)
	}

	for scan() {
		b := bucket
		if computeBucket {
			b = collector.TimeBucket(time.UnixMilli(ts))
		}

		var err error
		if ttl > 0 {
			err = session.Query(insertTTL, svc, metric, b, ts, instance, mtype, tags, labels, value, ttl).Exec()
		} else {
			// TTL 0/null: satirin suresi yok. Tablonun varsayilan TTL'ini
			// devralmamasi icin USING TTL 0 degil, TTL'siz yaziyoruz -
			// ama tablo default_time_to_live tasidigi icin yine de
			// varsayilani alir. Bu bilincli: suresiz metrik istemiyoruz.
			err = session.Query(insert, svc, metric, b, ts, instance, mtype, tags, labels, value).Exec()
		}
		if err != nil {
			_ = iter.Close()
			return copied, fmt.Errorf("satir yazilamadi (%s.%s @%d): %w", svc, metric, ts, err)
		}

		copied++
		if copied%50000 == 0 {
			fmt.Printf("      ... %d satir\n", copied)
		}
		tags, labels = nil, nil
	}

	if err := iter.Close(); err != nil {
		return copied, err
	}
	return copied, nil
}

func contains(list []string, want string) bool {
	for _, s := range list {
		if s == want {
			return true
		}
	}
	return false
}
