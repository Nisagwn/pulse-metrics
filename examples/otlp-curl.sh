#!/usr/bin/env bash
# PulseMetrics'e HIC GO KULLANMADAN veri gonderir.
#
# Bu betigin varlik sebebi bir seyi kanitlamak: Faz 5'ten sonra
# PulseMetrics'e veri gondermek icin ne bu deponun SDK'sina ne de Go'ya
# ihtiyac var. Duz HTTP ve JSON yetiyor - yani her dil, hatta kabuk.
#
# Gercek bir uygulamada bunlari elle yazmazsin; dilinin OpenTelemetry
# SDK'si ayni istekleri senin icin uretir. Ornegin Python'da:
#
#   OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318 \
#   OTEL_SERVICE_NAME=odeme-servisi \
#   opentelemetry-instrument python app.py
#
# Kullanimi:  ./examples/otlp-curl.sh
set -euo pipefail

OTLP=${OTLP_ENDPOINT:-http://localhost:4318}

# Zaman damgalari NANOSANIYE (OTLP boyle istiyor).
now_ns() { echo "$(date +%s)000000000"; }
NOW=$(now_ns)
START=$((NOW - 250000000))   # 250 ms once

# Kimlikler ONALTILIK: trace_id 32, span_id 16 karakter.
# (Protobuf'un JSON eslemesi base64 ister ama OTLP belirtimi bu iki alan
#  icin ozellikle onaltiliktan yana sapiyor - gateway ikisini de kabul eder.)
TRACE_ID="4bf92f3577b34da6a3ce929d0e0e4736"
GATEWAY_SPAN="00f067aa0ba902b7"
INVENTORY_SPAN="b7ad6b7169203331"

post() {
  local path=$1 body=$2
  echo -n "  ${path}  -> "
  curl -sS -o /tmp/otlp-resp.json -w "HTTP %{http_code}  " \
    -X POST "${OTLP}${path}" \
    -H "Content-Type: application/json" \
    -d "$body"
  cat /tmp/otlp-resp.json; echo
}

echo "PulseMetrics'e OTLP gonderiliyor: ${OTLP}"
echo

# --------------------------------------------------------------------
# 1) TRACE: iki servis, ebeveyn-cocuk iliskisi
#
# peer.service ozniteligine dikkat: OTel'in STANDART ozniteligi ve
# PulseMetrics servis haritasini bundan cikariyor. Yani harici bir SDK
# hicbir ozel ayar yapmadan topolojiye katkida bulunuyor.
# --------------------------------------------------------------------
post /v1/traces "$(cat <<EOF
{"resourceSpans":[
 {"resource":{"attributes":[
    {"key":"service.name","value":{"stringValue":"python-checkout"}},
    {"key":"service.instance.id","value":{"stringValue":"pod-a1"}},
    {"key":"deployment.environment","value":{"stringValue":"prod"}}]},
  "scopeSpans":[{"scope":{"name":"opentelemetry.instrumentation.flask"},
   "spans":[
    {"traceId":"${TRACE_ID}","spanId":"${GATEWAY_SPAN}",
     "name":"POST /checkout","kind":2,
     "startTimeUnixNano":"${START}","endTimeUnixNano":"${NOW}",
     "attributes":[
       {"key":"http.method","value":{"stringValue":"POST"}},
       {"key":"http.status_code","value":{"intValue":"500"}}],
     "status":{"code":2,"message":"stok yetersiz"}}]}]},

 {"resource":{"attributes":[
    {"key":"service.name","value":{"stringValue":"java-inventory"}},
    {"key":"service.instance.id","value":{"stringValue":"pod-b2"}}]},
  "scopeSpans":[{"scope":{"name":"io.opentelemetry.spring-webmvc"},
   "spans":[
    {"traceId":"${TRACE_ID}","spanId":"${INVENTORY_SPAN}",
     "parentSpanId":"${GATEWAY_SPAN}",
     "name":"GET /stok/{sku}","kind":2,
     "startTimeUnixNano":"$((START + 20000000))","endTimeUnixNano":"$((NOW - 30000000))",
     "attributes":[
       {"key":"peer.service","value":{"stringValue":"python-checkout"}},
       {"key":"db.system","value":{"stringValue":"postgresql"}}],
     "status":{"code":2,"message":"SKU-42 tukendi"},
     "events":[{"name":"exception","timeUnixNano":"$((NOW - 40000000))",
       "attributes":[{"key":"exception.type","value":{"stringValue":"OutOfStock"}}]}]}]}]}
]}
EOF
)"

# --------------------------------------------------------------------
# 2) LOG: ayni trace_id ile - uc ayagi birbirine baglayan sey
# --------------------------------------------------------------------
post /v1/logs "$(cat <<EOF
{"resourceLogs":[
 {"resource":{"attributes":[
    {"key":"service.name","value":{"stringValue":"java-inventory"}},
    {"key":"host.name","value":{"stringValue":"pod-b2"}}]},
  "scopeLogs":[{"scope":{"name":"com.example.StockService"},
   "logRecords":[
    {"timeUnixNano":"$((NOW - 35000000))","severityNumber":17,"severityText":"ERROR",
     "body":{"stringValue":"SKU-42 icin stok bulunamadi, talep 3 adet"},
     "traceId":"${TRACE_ID}","spanId":"${INVENTORY_SPAN}",
     "attributes":[{"key":"sku","value":{"stringValue":"SKU-42"}}]},
    {"timeUnixNano":"${NOW}","severityNumber":9,"severityText":"INFO",
     "body":{"stringValue":"stok sorgusu tamamlandi"},
     "traceId":"${TRACE_ID}","spanId":"${INVENTORY_SPAN}"}]}]}
]}
EOF
)"

# --------------------------------------------------------------------
# 3) METRIK: gauge, sayac ve bir histogram
#
# Histogram kovalarinin ayri metrik adlarina acildigini gormek icin
# panelde "seriler" listesine bak.
# --------------------------------------------------------------------
post /v1/metrics "$(cat <<EOF
{"resourceMetrics":[
 {"resource":{"attributes":[
    {"key":"service.name","value":{"stringValue":"python-checkout"}},
    {"key":"service.instance.id","value":{"stringValue":"pod-a1"}}]},
  "scopeMetrics":[{"scope":{"name":"app.metrics"},
   "metrics":[
    {"name":"process.threads",
     "gauge":{"dataPoints":[{"timeUnixNano":"${NOW}","asInt":"24"}]}},

    {"name":"checkout.istekleri",
     "sum":{"isMonotonic":true,"aggregationTemporality":2,
      "dataPoints":[{"timeUnixNano":"${NOW}","asInt":"1523",
        "attributes":[{"key":"sonuc","value":{"stringValue":"hata"}}]}]}},

    {"name":"checkout.suresi",
     "histogram":{"aggregationTemporality":2,
      "dataPoints":[{"timeUnixNano":"${NOW}",
        "count":"10","sum":2.75,
        "explicitBounds":[0.1,0.5,1.0],
        "bucketCounts":["4","3","2","1"]}]}}]}]}
]}
EOF
)"

echo
echo "Gonderildi. Simdi:"
echo "  Panel        http://localhost:8080"
echo "  Trace        curl \"localhost:8080/api/v1/trace?id=${TRACE_ID}\""
echo "  Trace loglar curl \"localhost:8080/api/v1/trace-logs?id=${TRACE_ID}\""
echo "  Topoloji     curl \"localhost:8080/api/v1/topology?range=1h\""
