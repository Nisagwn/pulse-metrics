-- PulseMetrics sema tanimi.
-- Collector acilista bunun aynisini kod icinden de calistirir
-- (internal/collector/collector.go, ensureSchema), yani bu dosyayi
-- elle calistirmak zorunda degilsin. Semanin okunabilir tek kaynagi olsun diye burada.

-- Replikasyon: gelistirme icin SimpleStrategy/1.
-- Uretimde ortam degiskenleriyle degistirilir:
--   PULSE_SCYLLA_REPLICATION_CLASS=NetworkTopologyStrategy
--   PULSE_SCYLLA_REPLICATION_FACTOR=3
--   PULSE_SCYLLA_LOCAL_DC=dc1
-- SimpleStrategy rack/DC farkindaligi olmadan kopya yerlestirir; cok
-- DC'li bir kumede iki kopya ayni DC'ye dusebilir ve o DC kaybedilince
-- veri gider.
CREATE KEYSPACE IF NOT EXISTS pulse
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

-- metrics: zaman serisi olcumleri.
--
-- PRIMARY KEY ((service_name, metric_name, time_bucket), timestamp, instance_id)
--   partition key  = (service_name, metric_name, time_bucket)
--       Verinin hangi dugumde duracagini belirler.
--
--       time_bucket FAZ 4'TE EKLENDI ve Faz 1'den beri tasinan bir borcu
--       kapatti. Eskiden bir metrigin TUM gecmisi tek partition'daydi ve
--       TTL doluncaya kadar durmadan buyuyordu: 10 saniyede bir olcum
--       gonderen 10 instance, 30 gunde tek partition'da ~2.6 milyon satir.
--       Cassandra/Scylla'da bu, o partition'i tutan dugumu sicak nokta
--       haline getirir; okuma gecikmesi ve compaction maliyeti partition
--       boyutuyla birlikte buyur.
--
--       Saatlik kova bunu sinirlar. Bedeli, "son 24 saat" sorgusunun 24
--       partition okumasi - Cassandra icin tamamen olagan bir erisim
--       bicimi. Gunluk kova daha az partition okurdu ama tek partition'i
--       24 kat buyuturdu.
--
--   clustering key = timestamp DESC, instance_id ASC
--       timestamp once geldigi icin "son 1 saat" sorgusu partition'in
--       basindan sirali okuma yapar. instance_id sondadir ve satirlari
--       benzersiz kilar: ayni servisin iki instance'i ayni milisaniyede
--       olcum gonderirse artik birbirinin uzerine yazmaz.
--
-- ESKI SEMADAN GECIS: partition key degistirilemez, bu bir ALTER degil
-- TASIMA islemidir. cmd/pulse-migrate bunu yapar; docs/OPERATIONS.md'ye bak.
CREATE TABLE IF NOT EXISTS pulse.metrics (
    service_name TEXT,
    metric_name  TEXT,
    time_bucket  TEXT,      -- 'YYYYMMDDHH', UTC
    timestamp    BIGINT,
    instance_id  TEXT,
    type         TEXT,
    tags         MAP<TEXT, TEXT>,
    labels       MAP<TEXT, TEXT>,
    value        DOUBLE,
    PRIMARY KEY ((service_name, metric_name, time_bucket), timestamp, instance_id)
) WITH CLUSTERING ORDER BY (timestamp DESC, instance_id ASC)
  AND default_time_to_live = 2592000;  -- 30 gun sonra satir kendini siler
