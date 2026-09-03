# PulseMetrics — İşletim Rehberi

Faz 4 ile gelen belge. Sistemi yazmak ayrı, **ayakta tutmak** ayrı bir iş;
bu dosya ikincisiyle ilgili.

---

## 1. Bileşenler ve portlar

| Bileşen | Port | Ne yapar |
|---|---|---|
| `agent` | 8081 (sağlık) | Süreç metriklerini toplar, Kafka'ya yazar |
| `collector` | 50051 (gRPC), 8082 (sağlık) | Kafka → ScyllaDB, sorgu API'si, alarm motoru |
| `dashboard-api` | 8080 | gRPC → JSON köprüsü + panel |
| `demo` | — | Örnek enstrümante servisler + yük üreteci |
| `pulse-migrate` | — | Şema göçü aracı |

Her bileşen üç yönetim ucu yayınlar. Ayrımları önemli:

```
/healthz   canlılık   "Süreç cevap veriyor mu?"
                      Başarısızsa orkestratör süreci YENİDEN BAŞLATIR.
                      Bağımlılıkları KONTROL ETMEZ - bilerek.

/readyz    hazırlık   "İş yapabilir durumda mı?" ScyllaDB/Kafka kontrol eder.
                      Başarısızsa orkestratör trafiği keser ama öldürmez.

/metrics   ölçüm      Prometheus formatında kendi ölçüleri.
```

Ayrımı kaçırmak klasik bir hatadır: ScyllaDB geçici olarak düştüğünde
`/healthz` de başarısız olursa Kubernetes **tüm** collector'ları sonsuz
döngüde yeniden başlatır ve veritabanı geri geldiğinde ayakta kimse kalmaz.

---

## 2. Yapılandırma

Öncelik sırası: **CLI bayrağı > ortam değişkeni > varsayılan**.

Bayrak en üstte çünkü elle müdahale her zaman kazanmalı; ortam değişkeni
ortada çünkü Kubernetes bir Pod'a bayrak değil ortam değişkeni geçirir ve
aynı imajın dev/staging/prod'da farklı davranması böyle sağlanır.

### Ortak

| Değişken | Varsayılan | Açıklama |
|---|---|---|
| `PULSE_KAFKA_BROKERS` | `localhost:9092` | Virgülle ayrılmış |
| `PULSE_INSTANCE_ID` | hostname | Bu sürecin kümedeki adı |
| `PULSE_HEALTH_ADDR` | bileşene göre | Sağlık/ölçüm HTTP adresi |
| `PULSE_DEBUG` | `false` | Ayrıntılı log |

### ScyllaDB

| Değişken | Varsayılan | Açıklama |
|---|---|---|
| `PULSE_SCYLLA_HOSTS` | `localhost:9042` | **Üretimde en az üç adres ver** |
| `PULSE_SCYLLA_KEYSPACE` | `pulse` | |
| `PULSE_SCYLLA_CONSISTENCY` | `QUORUM` | Çok DC'de `LOCAL_QUORUM` |
| `PULSE_SCYLLA_REPLICATION_CLASS` | `SimpleStrategy` | Üretimde `NetworkTopologyStrategy` |
| `PULSE_SCYLLA_REPLICATION_FACTOR` | `1` | Üretimde `3` |
| `PULSE_SCYLLA_LOCAL_DC` | — | `NetworkTopologyStrategy` ve `LOCAL_QUORUM` için zorunlu |
| `PULSE_SCYLLA_NUM_CONNS` | `2` | Host başına bağlantı |

**Neden tek host yetmez:** sürücü gossip ile diğer düğümleri öğrenir, ama
o **tek** adres açılışta kapalıysa bağlantı hiç kurulamaz.

**Neden `SimpleStrategy` üretimde yanlış:** rack/DC farkındalığı olmadan
kopya yerleştirir. Çok DC'li bir kümede iki kopya aynı DC'ye düşebilir ve
o DC kaybedildiğinde veri gider.

**Neden çok DC'de `QUORUM` değil `LOCAL_QUORUM`:** `QUORUM` kümenin
tamamında çoğunluk arar, yani okyanus aşırı onay bekler. Her yazma
yüzlerce milisaniye sürer.

Ayarlar **açılışta doğrulanır**. Yanlış bir ayarla açılmaktansa hiç
açılmamak doğru davranış: yanlış ayarla açılan bir süreç orkestratöre
"sağlıklıyım" der ve sorun görünmez kalır.

```bash
# Üretim örneği
PULSE_SCYLLA_HOSTS=scylla-1:9042,scylla-2:9042,scylla-3:9042 \
PULSE_SCYLLA_REPLICATION_CLASS=NetworkTopologyStrategy \
PULSE_SCYLLA_REPLICATION_FACTOR=3 \
PULSE_SCYLLA_LOCAL_DC=dc1 \
PULSE_SCYLLA_CONSISTENCY=LOCAL_QUORUM \
./bin/collector
```

---

## 3. Yatay ölçeklendirme

### Collector'ı çoğaltmak

```bash
PULSE_INSTANCE_ID=collector-1 ./bin/collector -port 50051 -health :8082
PULSE_INSTANCE_ID=collector-2 ./bin/collector -port 50053 -health :8084
```

İkisi aynı consumer group'ta olduğu için Kafka partition'ları aralarında
**bölüştürür**; aynı mesajı ikisi birden işlemez. Doğrulamak için:

```bash
docker exec pulse-kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 --describe --group pulse-collector
```

Farklı `CONSUMER-ID` değerleri görüyorsan iş gerçekten bölüşülüyor.

### Partition sayısı tavandır

> Bir consumer group'ta aynı anda en fazla **partition sayısı** kadar
> tüketici iş yapabilir.

Tek partition'lı bir topic'te ikinci collector'ı açsan da boş oturur.
Faz 4'e kadar varsayılan 1'di; artık 3:

```bash
make topics    # mevcut topic'leri 3 partition'a çıkarır
```

Partition sayısını sonradan artırmak mümkün ama `anahtar → partition`
eşlemesini değiştirir; metrikler için bu kabul edilebilir, sıra garantisine
dayanan sistemlerde değildir.

### Alarmlar neden iki kez gitmiyor?

Faz 3'te "hangi kural şu anda tetiklenmiş" bilgisi her collector'ın
**belleğindeydi**. İki collector = iki bellek = her alarm iki kez.
Sistem çökmüyordu; daha sinsi bir şey yapıyordu.

Faz 4'te durum `pulse.alert_state` tablosunda ve geçiş **LWT** (CQL'in
`IF` cümlesi) ile yapılıyor:

```sql
UPDATE alert_state SET firing = true, ... WHERE rule_id = ? IF firing = false
```

Sıradan bir `UPDATE` "son yazan kazanır" mantığıyla çalışır — ikisi de
başarılı olur. `IF` eklendiğinde Scylla arka planda Paxos çalıştırır ve
güncellemeyi **yalnızca** mevcut değer beklediğimiz değerse uygular.
Yarıştan tek kazanan çıkar; kaybeden sessizce geçer.

LWT pahalıdır (dört gidiş-dönüş + düğümler arası uzlaşma), bu yüzden
**sadece durum geçişinde** kullanılır. Sıradan turlar önce ucuz bir
`SELECT` yapar ve durum değişmemişse Paxos'a hiç girmez.

Canlı doğrulama:

```bash
curl -s localhost:8082/metrics | grep pulse_alert_transitions
curl -s localhost:8084/metrics | grep pulse_alert_transitions
```

Aynı geçiş için birinde `result="won"`, diğerinde `result="lost"` görürsün.
Sakin turlar hiç sayılmaz — sayaçtaki her satır gerçek bir olaydır.

---

## 4. Şema göçü

### Neden bir araç gerekiyor?

Cassandra ve Scylla'da **partition key değiştirilemez**. Kolon eklenebilir,
TTL değiştirilebilir; ama verinin hangi düğüme düşeceğini belirleyen
anahtara dokunulamaz, çünkü bu tüm verinin kümede yeniden dağıtılması
demek. Faz 4'ün değişikliği tam olarak bu:

```
Faz 1-3: PRIMARY KEY ((service_name, metric_name), timestamp, instance_id)
Faz 4:   PRIMARY KEY ((service_name, metric_name, time_bucket), timestamp, instance_id)
```

Yani bir `ALTER` değil, bir **taşıma**.

### Durdurmalı göç (bu proje ölçeği)

```bash
# 1. Planı gör - hiçbir şey değişmez
make migrate-dry

# 2. Collector'ları DURDUR (göç sırasında tablo kısa süre yok olur)

# 3. Çalıştır
make migrate

# 4. Doğrula, sonra ara tabloyu sil
docker exec pulse-scylladb cqlsh -e "SELECT COUNT(*) FROM pulse.metrics;"
docker exec pulse-scylladb cqlsh -e "DROP TABLE pulse.metrics_v4;"
```

Araç TTL'i **korur**: `TTL(value)` ile kalan süreyi okur, `USING TTL` ile
yazar. Yapılmasaydı taşınan her satırın sayacı sıfırlanır, 29 günlük bir
ölçüm 30 gün daha yaşardı — sessiz ama gerçek bir hata: tablo küçülmesi
gerekirken büyümeye devam ederdi.

Araç **idempotent**: şema zaten güncelse hiçbir şey yapmadan çıkar.

### Kesintisiz göç (gerçek üretim)

Büyük bir kümede yukarıdakini kullanma. Doğrusu:

1. Collector'ı her iki tabloya da yazacak şekilde dağıt (dual-write)
2. Okumayı önce eskiye, sonra yeniye çevir
3. TTL süresi kadar bekle (burada 30 gün) — eski tablo kendini boşaltır
4. Eski tabloyu sil

Hiçbir adımda kesinti yok ve kopyalama hiç yapılmıyor; **zamanın kendisi**
göçü tamamlıyor. Bedeli 30 gün boyunca iki kat yazma.

### Yanlış şemayla açılma koruması

`CREATE TABLE IF NOT EXISTS` var olan bir tabloyu **değiştirmez**; sessizce
hiçbir şey yapar. Collector bu yüzden açılışta şemayı **doğrular** ve eski
şema bulursa açılmayı reddeder:

```
metrics tablosu ESKI semada (partition key: service_name, metric_name).
Faz 4 time_bucket bekliyor.
  Gocu calistir:  go run ./cmd/pulse-migrate -scylla pulse -confirm
```

Alternatif daha kötü olurdu: yanlış şemayla açılan collector orkestratöre
"sağlıklıyım" der, Kafka'dan okumaya devam eder ve okuduğu her mesajı
yazamadan atar. **Sessiz veri kaybı, gürültülü bir açılış hatasından her
zaman daha pahalıdır.**

---

## 5. Öz-izleme

Bir izleme sisteminin en can sıkıcı arızası **sessizce durmasıdır**:
panelde grafik düz çizgi olur ve bunun "trafik yok" mu "collector öldü" mü
olduğunu kimse bilemez.

PulseMetrics kendini izlemez — dairesel bağımlılık olurdu. Collector
çöktüğünde haber verecek şey **başka** bir sistem olmalı:
`docker-compose.yml`'deki Prometheus tam olarak bu işi yapıyor.

```bash
make metrics                          # ham çıktı
open http://localhost:9090/targets    # Prometheus hedefleri
open http://localhost:3000            # Grafana (admin / admin)
```

Grafana'da **PulseMetrics → Collector** panosu dosyadan yüklenir
(`config/dashboards/`), yani sürüm kontrolünde durur ve konteyner silinip
yeniden kurulunca kaybolmaz.

### Önemli ölçüler

| Ölçü | Ne söyler |
|---|---|
| `pulse_collector_kafka_lag` | **Üretimde bakılacak ilk grafik.** Sürekli artıyorsa collector üretimden yavaş kalıyor |
| `pulse_collector_records_ingested_total` | Gerçek hacim (mesaj değil kayıt) |
| `pulse_collector_ingest_errors_total` | `kind`: `decode` = bozuk mesaj, `read` = Kafka, `write` = ScyllaDB |
| `pulse_collector_ingest_duration_seconds` | Artıyorsa darboğaz genelde ScyllaDB yazma gecikmesi |
| `pulse_alert_transitions_total` | `won`/`lost` — yatay ölçeklendirmenin çalıştığının kanıtı |
| `go_goroutines` | Düzenli artış = goroutine sızıntısı |
| `pulse_build_info` | "Hangi sürüm çalışıyor?" |

Ölçüler açılışta **sıfırla başlatılır**. Etiketli bir ölçü, o etiketle ilk
kez yazılana kadar `/metrics` çıktısında hiç görünmez; ilk kazıma boş
dönerse Prometheus tarafında "seri yok" ile "değer sıfır" ayırt edilemez ve
`rate()` ile alarm kuralları eksik seri karşısında sessizce hiçbir şey yapar.

---

## 6. Konteynerle dağıtım

```bash
docker compose up -d                                    # altyapı
docker compose -f docker-compose.apps.yml up -d --build # uygulamalar (2 collector)
```

İmaj iki aşamalı: derleme aşamasında Go araç zinciri var, çalışma
aşamasında yok. Sonuç, `distroless/static` tabanlı, kabuk ve paket
yöneticisi içermeyen, **root olmayan** kullanıcıyla çalışan tek dosyalık
bir imaj. Kabuk olmaması üretimde bir özelliktir: konteynere `exec` ile
girip komut çalıştırılamaz.

Konteyner sağlık kontrolü `/app -version` kullanır — distroless'ta `curl`
yok. Bağımlılık kontrolü `/readyz`'de; Kubernetes'te `readinessProbe` onu
kullanmalı:

```yaml
livenessProbe:
  httpGet: { path: /healthz, port: 8082 }
readinessProbe:
  httpGet: { path: /readyz, port: 8082 }
```

---

## 7. Sorun giderme

**Collector açılmıyor, "ESKI semada" diyor**
→ Bölüm 4. Göçü çalıştır.

**Collector açılmıyor, "Ayar hatasi" diyor**
→ Hata mesajı hangi ayarın neden reddedildiğini söyler. En sık görülen:
üç host verilip `replication_factor` 1 bırakılması (bir düğüm kaybında veri
gider) veya `LOCAL_QUORUM` verilip `PULSE_SCYLLA_LOCAL_DC` verilmemesi.

**İkinci collector açıldı ama hiç mesaj işlemiyor**
→ Partition sayısı 1'dir. `make topics`.

**Kafka gecikmesi sürekli artıyor**
→ Sırayla: (1) `pulse_collector_ingest_duration_seconds` p95 arttı mı?
ScyllaDB yavaşlıyordur. (2) Partition sayısı collector sayısından az mı?
(3) İkisi de değilse collector ekle.

**Alarm iki kez geldi**
→ `pulse.alert_state` tablosu var mı? Yoksa Faz 4 şeması uygulanmamıştır;
collector'ı yeniden başlat (açılışta yaratır).

**Alarm hiç gelmiyor ama koşul ihlal ediliyor**
→ `alert_state` içinde kuralın satırı `firing=true` kalmış olabilir
(örneğin süreç bildirimi gönderemeden öldü). Kontrol et:
```sql
SELECT * FROM pulse.alert_state WHERE rule_id = '...';
```
Gerekirse satırı sil; bir sonraki turda yeniden tetiklenir.

**Grafana boş**
→ `http://localhost:9090/targets` — hedefler `UP` mı? Süreçler makinede
çalışıyorsa `config/prometheus.yml` (host.docker.internal), konteynerde
çalışıyorsa `config/prometheus-apps.yml` geçerli olmalı.

**Panelde operasyon adları `/other` görünüyor**
→ Kardinalite tavanı devreye girdi (varsayılan 500 farklı ad).
Normalleştirme yetmiyor demektir; yönlendirici şablonunu ver:
```go
tracer.Middleware(router, tracing.WithOperationName(func(r *http.Request) string {
    return r.Method + " " + chi.RouteContext(r.Context()).RoutePattern()
}))
```

---

## 8. Kapasite notları

| Tablo | Partition anahtarı | TTL | Sınırlı mı? |
|---|---|---|---|
| `metrics` | `(service, metric, saat)` | 30 gün | ✅ |
| `spans` | `trace_id` | 7 gün | ✅ (trace başına sınırlı span) |
| `trace_index` | `(service, saat)` | 7 gün | ✅ |
| `logs` | `(service, saat)` | 7 gün | ✅ |
| `service_edges` | `(çağıran, çağrılan, saat)` | 7 gün | ✅ |
| `alerts` | `(service, saat)` | 30 gün | ✅ |
| `alert_rules` | `rule_id` | yok | ✅ (satır başına partition) |
| `alert_state` | `rule_id` | **yok, olmamalı** | ✅ |

`alert_state`'in TTL'i bilerek yok: bu tablo veri değil **karar** tutuyor.
Bir alarmın tetiklenmiş olduğu bilgisi 7 gün sonra kendiliğinden silinirse,
alarm hâlâ devam ederken sistem onu "yeni" sanıp tekrar bildirir.

Saatlik kova seçimi: "son 24 saat" sorgusu 24 partition okur — Cassandra
için tamamen olağan bir erişim biçimi. Günlük kova daha az partition okurdu
ama tek partition'ı 24 kat büyütürdü.
