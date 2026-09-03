# PulseMetrics ikili dosyalari icin ortak Dockerfile.
#
# Hangi ikilinin derlenecegi CMD_NAME ile secilir:
#   docker build --build-arg CMD_NAME=collector -t pulse/collector .
#   docker build --build-arg CMD_NAME=agent     -t pulse/agent .
#
# Iki asamali: derleme asamasinda Go arac zinciri var, calisma asamasinda
# yok. Sonuc, ~20 MB'lik tek dosyalik bir imaj - icinde kabuk, paket
# yoneticisi, derleyici bulunmayan bir imaj saldiri yuzeyi olarak da
# kucuktur.

# ---------- 1) derleme ----------
FROM golang:1.26-alpine AS build

WORKDIR /src

# Once sadece bagimlilik dosyalari kopyalanir. Kaynak kodu degistiginde
# bu katman degismedigi icin Docker onbellekten "go mod download"
# adimini atlar; her derlemede modulleri yeniden indirmek yerine.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG CMD_NAME=collector
ARG VERSION=dev
ARG COMMIT=""

# CGO_ENABLED=0: tamamen statik ikili. Boylece calisma asamasinda libc
# gerekmiyor ve distroless/scratch tabanli imaj kullanilabiliyor.
# -trimpath: derleyen makinenin dizin yollarini ikiliye gomme.
# -s -w: hata ayiklama sembollerini at, imaji kucult.
RUN CGO_ENABLED=0 GOOS=linux go build \
	-trimpath \
	-ldflags "-s -w \
		-X github.com/nisah/pulse-metrics/internal/buildinfo.Version=${VERSION} \
		-X github.com/nisah/pulse-metrics/internal/buildinfo.Commit=${COMMIT}" \
	-o /out/app ./cmd/${CMD_NAME}

# ---------- 2) calisma ----------
# distroless/static: sadece CA sertifikalari, saat dilimi verisi ve
# root olmayan bir kullanici. Kabuk yok - konteynere "exec" ile girip
# komut calistirilamaz, ki uretimde bu bir ozelliktir.
FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /out/app /app

# nonroot kullanicisi (uid 65532). Konteyner root olarak calismamali:
# bir kacis acigi durumunda ana makinede root olmayi engeller.
USER nonroot:nonroot

# 8082 saglik/olcum, 50051 gRPC. Sadece belgeleme amacli.
EXPOSE 8080 8081 8082 50051

ENTRYPOINT ["/app"]
