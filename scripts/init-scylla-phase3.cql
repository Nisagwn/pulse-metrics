-- PulseMetrics Faz 3 semasi: loglar, servis kenarlari, alarmlar.
-- Collector acilista bunlarin aynisini kod icinden de calistirir.

-- ---------------------------------------------------------------------------
-- 1) logs: "son 1 saatte payments servisinde ERROR seviyeli loglar"
--
-- Faz 2'de ogrenilen ders burada bastan uygulaniyor: partition key'de
-- saatlik time_bucket var, yani partition'lar sinirsiz buyumuyor.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pulse.logs (
    service_name TEXT,
    time_bucket  TEXT,      -- 'YYYYMMDDHH'
    timestamp_ms BIGINT,
    log_id       TEXT,      -- ayni ms'de gelen loglari ayirir
    level        TEXT,
    message      TEXT,
    trace_id     TEXT,
    span_id      TEXT,
    logger_name  TEXT,
    instance_id  TEXT,
    stack_trace  TEXT,
    attributes   MAP<TEXT, TEXT>,
    PRIMARY KEY ((service_name, time_bucket), timestamp_ms, log_id)
) WITH CLUSTERING ORDER BY (timestamp_ms DESC, log_id ASC)
  AND default_time_to_live = 604800;  -- 7 gun

-- ---------------------------------------------------------------------------
-- 2) trace_logs: "su trace boyunca uretilen tum loglar"
--
-- Ucuncu ayagi (log) ikinci ayaga (trace) baglayan tablo. Ayni log iki kez
-- yaziliyor cunku iki farkli soru soruluyor - Faz 2'deki spans/trace_index
-- ikilisiyle ayni mantik.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pulse.trace_logs (
    trace_id     TEXT,
    timestamp_ms BIGINT,
    log_id       TEXT,
    service_name TEXT,
    span_id      TEXT,
    level        TEXT,
    message      TEXT,
    logger_name  TEXT,
    attributes   MAP<TEXT, TEXT>,
    PRIMARY KEY (trace_id, timestamp_ms, log_id)
) WITH CLUSTERING ORDER BY (timestamp_ms ASC, log_id ASC)
  AND default_time_to_live = 604800;

-- ---------------------------------------------------------------------------
-- 3) log_services: arama formundaki servis listesi
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pulse.log_services (
    service_name TEXT PRIMARY KEY,
    last_seen_ms BIGINT
) WITH default_time_to_live = 604800;

-- ---------------------------------------------------------------------------
-- 4) service_edges: servisler arasi cagrilar, INGEST sIRASINDA yazilir
--
-- Faz 2'de topoloji sorgu aninda hesaplaniyordu: son N trace ornekleniyor,
-- her biri okunuyor, ebeveyn-cocuk baglari yuruunuyordu. Demo olceginde
-- calisiyordu ama her GetTopology cagrisi onlarca partition okumasi demekti.
--
-- Faz 3'te kaynaktan cozuluyor: SDK giden istege kendi servis adini
-- tracestate icinde tasiyor (W3C'nin tam olarak bunun icin ayirdigi alan),
-- sunucu tarafi bunu peer.service ozniteligi olarak kaydediyor. Boylece
-- collector bir SERVER span'i gordugunde cagiranin kim oldugunu EK BIR
-- OKUMA YAPMADAN biliyor ve kenari dogrudan buraya yaziyor.
--
-- Yuzdelikler icin sureleri sakliyoruz: COUNTER tablosu sayabilir ama
-- p95 hesaplayamaz.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pulse.service_edges (
    caller_service  TEXT,
    callee_service  TEXT,
    time_bucket     TEXT,     -- 'YYYYMMDDHH'
    timestamp_ms    BIGINT,
    span_id         TEXT,
    duration_ms     BIGINT,
    has_error       BOOLEAN,
    PRIMARY KEY ((caller_service, callee_service, time_bucket), timestamp_ms, span_id)
) WITH CLUSTERING ORDER BY (timestamp_ms DESC, span_id ASC)
  AND default_time_to_live = 604800;

-- edge_pairs: hangi (cagiran, cagrilan) ciftleri var? Grafigi cizmek icin
-- once bunu okuyup sonra service_edges'e gidiyoruz.
CREATE TABLE IF NOT EXISTS pulse.edge_pairs (
    time_bucket    TEXT,
    caller_service TEXT,
    callee_service TEXT,
    last_seen_ms   BIGINT,
    PRIMARY KEY (time_bucket, caller_service, callee_service)
) WITH default_time_to_live = 604800;

-- ---------------------------------------------------------------------------
-- 5) alert_rules: alarm kurallari (kalici, TTL yok)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pulse.alert_rules (
    rule_id          TEXT PRIMARY KEY,
    name             TEXT,
    service_name     TEXT,
    metric_name      TEXT,
    condition        TEXT,     -- "p95 > 500", "zscore > 3"
    duration_seconds INT,
    webhook_url      TEXT,
    enabled          BOOLEAN,
    severity         TEXT,
    created_at_ms    BIGINT
);

-- ---------------------------------------------------------------------------
-- 6) alerts: tetiklenen alarmlar
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pulse.alerts (
    service_name TEXT,
    time_bucket  TEXT,
    timestamp_ms BIGINT,
    alert_id     TEXT,
    rule_id      TEXT,
    rule_name    TEXT,
    metric_name  TEXT,
    condition    TEXT,
    metric_value DOUBLE,
    threshold    DOUBLE,
    severity     TEXT,
    message      TEXT,
    resolved     BOOLEAN,
    PRIMARY KEY ((service_name, time_bucket), timestamp_ms, alert_id)
) WITH CLUSTERING ORDER BY (timestamp_ms DESC, alert_id ASC)
  AND default_time_to_live = 2592000;  -- 30 gun
