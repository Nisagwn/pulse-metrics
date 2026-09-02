-- PulseMetrics trace semasi (Faz 2).
-- Collector acilista bunun aynisini kod icinden de calistirir
-- (internal/collector/traces.go, ensureTraceSchema).
--
-- Trace verisi metriklerden farkli bir sorgu deseni ister ve bu, iki ayri
-- tablo yazmayi gerektirir. Cassandra ailesinde tek bir tabloyla iki farkli
-- erisim desenini verimli karsilayamazsin; cozum ayni veriyi iki bicimde
-- yazmaktir (denormalizasyon).

-- ---------------------------------------------------------------------------
-- 1) spans: "su trace_id'nin butun span'lerini ver"
--
-- Trace goruntuleyici bunu kullanir. Bir trace'in tum span'leri tek
-- partition'da: bir trace kac servise ugrarsa ugrasin tek okuma.
--
-- Partition key sadece trace_id: trace'ler zaten dogal olarak kucuk
-- (onlarca span), yani partition buyumesi sorun degil.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pulse.spans (
    trace_id          TEXT,
    start_time_micros BIGINT,
    span_id           TEXT,
    parent_span_id    TEXT,
    service_name      TEXT,
    operation_name    TEXT,
    end_time_micros   BIGINT,
    duration_micros   BIGINT,
    kind              TEXT,
    status_code       TEXT,
    status_message    TEXT,
    attributes        MAP<TEXT, TEXT>,
    events            TEXT,      -- JSON dizisi; olaylar seyrek ve serbest bicimli
    instance_id       TEXT,
    PRIMARY KEY (trace_id, start_time_micros, span_id)
) WITH CLUSTERING ORDER BY (start_time_micros ASC, span_id ASC)
  AND default_time_to_live = 604800;  -- 7 gun (trace'ler metriklerden hacimli)

-- ---------------------------------------------------------------------------
-- 2) trace_index: "son 1 saatte checkout-service'te 500 ms'den yavas
--    olan istekleri bul"
--
-- Arama ekrani bunu kullanir. Burada partition key'e bir ZAMAN KOVASI
-- (time_bucket, "YYYYMMDDHH") koyuyoruz. Sebebi onemli:
--
--   Sadece service_name partition key olsaydi, yogun bir servisin
--   partition'i her gecen gun buyurdu ve sonunda tek bir dugumu
--   bogardi. Buna "unbounded partition" denir ve Cassandra ailesinde
--   en sik yapilan tasarim hatasidir.
--
--   Saatlik kova ile her partition en fazla bir saatlik veri tutar.
--   Karsiliginda 6 saatlik bir sorgu 6 partition okur - kabul edilebilir
--   bir takas.
--
-- NOT: Faz 1'deki pulse.metrics tablosu hala bu kovadan yoksun. Gercek
-- trafikte calistirmadan once oraya da eklenmeli.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pulse.trace_index (
    service_name      TEXT,
    time_bucket       TEXT,      -- 'YYYYMMDDHH'
    start_time_micros BIGINT,
    trace_id          TEXT,
    span_id           TEXT,
    operation_name    TEXT,
    duration_micros   BIGINT,
    has_error         BOOLEAN,
    root              BOOLEAN,   -- trace'in kok span'i mi?
    PRIMARY KEY ((service_name, time_bucket), start_time_micros, trace_id, span_id)
) WITH CLUSTERING ORDER BY (start_time_micros DESC, trace_id ASC, span_id ASC)
  AND default_time_to_live = 604800;

-- ---------------------------------------------------------------------------
-- 3) service_ops: arama formundaki "servis" ve "operasyon" listeleri
--
-- SELECT DISTINCT partition key uzerinde calisir, yani bu tablo
-- servis/operasyon listesini ucuza verir.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pulse.service_ops (
    service_name   TEXT,
    operation_name TEXT,
    last_seen_ms   BIGINT,
    PRIMARY KEY (service_name, operation_name)
) WITH default_time_to_live = 604800;
