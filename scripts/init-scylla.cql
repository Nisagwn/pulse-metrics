-- PulseMetrics sema tanimi.
-- Collector acilista bunun aynisini kod icinden de calistirir
-- (internal/collector/collector.go, ensureSchema), yani bu dosyayi
-- elle calistirmak zorunda degilsin. Semanin okunabilir tek kaynagi olsun diye burada.

CREATE KEYSPACE IF NOT EXISTS pulse
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

-- metrics: zaman serisi olcumleri.
--
-- PRIMARY KEY ((service_name, metric_name), timestamp, instance_id)
--   partition key  = (service_name, metric_name)
--       Verinin hangi dugumde duracagini belirler. Bir metrigin tum
--       instance'lari ve tum zaman noktalari ayni partition'da, yan yana.
--   clustering key = timestamp DESC, instance_id ASC
--       timestamp once geldigi icin "son 1 saat" sorgusu partition'in
--       basindan siralı okuma yapar. instance_id sondadir ve satirlari
--       benzersiz kilar: ayni servisin iki instance'i ayni milisaniyede
--       olcum gonderirse artik birbirinin uzerine yazmaz.
CREATE TABLE IF NOT EXISTS pulse.metrics (
    service_name TEXT,
    metric_name  TEXT,
    timestamp    BIGINT,
    instance_id  TEXT,
    type         TEXT,
    tags         MAP<TEXT, TEXT>,
    labels       MAP<TEXT, TEXT>,
    value        DOUBLE,
    PRIMARY KEY ((service_name, metric_name), timestamp, instance_id)
) WITH CLUSTERING ORDER BY (timestamp DESC, instance_id ASC)
  AND default_time_to_live = 2592000;  -- 30 gun sonra satir kendini siler
