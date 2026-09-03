// Package otlp: OpenTelemetry Protocol verisini PulseMetrics tiplerine cevirir.
//
// NEDEN VAR?
//
// Faz 4'un sonuna kadar PulseMetrics'e veri gondermenin tek yolu bu
// deponun icindeki Go SDK'siniydi. Yani sistem yalnizca (a) Go ile
// yazilmis ve (b) bu depoda olan programlari izleyebiliyordu.
//
// OTLP, gozlemlenebilirlik dunyasinin ortak dili. Python, Java, Node,
// Rust, .NET - hepsinin resmi OpenTelemetry SDK'si var ve hepsi ayni
// protokolu konusuyor. Bu protokolu kabul etmek, "kendini izleyen bir
// proje"yi "her seyi izleyebilen bir sistem"e cevirir.
//
// # CEVIRININ ZORLUGU
//
// Iki veri modeli ayni seyi anlatmiyor. OTLP zengin ve ic ice: bir
// oznitelik degeri metin, sayi, dizi ya da ic ice harita olabilir;
// metrikler bes farkli sekilde gelebilir. PulseMetrics'in modeli duz:
// map<string,string> oznitelikler, tek degerli metrikler. Cevirinin isi
// bilgi kaybini gorunur ve bilincli kilmak - sessizce dusurmek degil.
package otlp

import (
	"encoding/base64"
	"encoding/hex"
	"strconv"
	"strings"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
)

// maxAttrDepth: ic ice haritalarda ne kadar derine inilir?
//
// Sinir olmasa kotu niyetli ya da hatali bir istemci derin ic ice bir
// yapi gonderip ceviriyi yiginda patlatabilirdi. Gozlemlenebilirlik
// verisi guvenilmeyen girdidir: onu gonderen uygulama senin degil.
const maxAttrDepth = 4

// Attributes: OTLP KeyValue listesini duz bir map[string]string'e cevirir.
//
// Ic ice haritalar noktali anahtarlarla duzlestirilir:
//
//	http.request.header = {accept: json}  ->  "http.request.header.accept": "json"
//
// Diziler tek bir metne cevrilir: [a b c] -> "[a,b,c]". Diziyi elemanlara
// bolmek de mumkundu (foo.0, foo.1) ama o zaman eleman sayisi kadar
// oznitelik olusur ve kardinalite kontrolden cikar.
func Attributes(kvs []*commonpb.KeyValue) map[string]string {
	if len(kvs) == 0 {
		return nil
	}
	out := make(map[string]string, len(kvs))
	flatten(out, "", kvs, 0)
	if len(out) == 0 {
		return nil
	}
	return out
}

func flatten(out map[string]string, prefix string, kvs []*commonpb.KeyValue, depth int) {
	for _, kv := range kvs {
		if kv == nil || kv.Key == "" {
			continue
		}
		key := kv.Key
		if prefix != "" {
			key = prefix + "." + key
		}

		// Ic ice harita: derinlik sinirina kadar duzlestir.
		if list, ok := kv.GetValue().GetValue().(*commonpb.AnyValue_KvlistValue); ok &&
			list.KvlistValue != nil && depth < maxAttrDepth {
			flatten(out, key, list.KvlistValue.Values, depth+1)
			continue
		}

		out[key] = AnyValueString(kv.GetValue())
	}
}

// AnyValueString: tek bir OTLP degerini metne cevirir.
//
// Kayipli bir donusum ve bunu kabul ediyoruz: PulseMetrics'in oznitelik
// tipi map<string,string>. Sayilar ve boolean'lar metne cevrildiginde
// uzerlerinde aritmetik yapamazsin - ama oznitelikler zaten filtreleme
// ve gruplama icin, hesaplama icin degil. Hesaplanacak sayi metriktir.
func AnyValueString(v *commonpb.AnyValue) string {
	if v == nil {
		return ""
	}
	switch val := v.GetValue().(type) {
	case *commonpb.AnyValue_StringValue:
		return val.StringValue
	case *commonpb.AnyValue_BoolValue:
		return strconv.FormatBool(val.BoolValue)
	case *commonpb.AnyValue_IntValue:
		return strconv.FormatInt(val.IntValue, 10)
	case *commonpb.AnyValue_DoubleValue:
		// 'g' bicimi gereksiz sifir kuyruklari birakmaz: 1.5 -> "1.5",
		// 100 -> "100" ("100.000000" degil).
		return strconv.FormatFloat(val.DoubleValue, 'g', -1, 64)
	case *commonpb.AnyValue_BytesValue:
		// Ham baytlar metne dogrudan cevrilemez; base64 tersine
		// cevrilebilir tek gosterim.
		return base64.StdEncoding.EncodeToString(val.BytesValue)
	case *commonpb.AnyValue_ArrayValue:
		if val.ArrayValue == nil {
			return "[]"
		}
		parts := make([]string, 0, len(val.ArrayValue.Values))
		for _, item := range val.ArrayValue.Values {
			parts = append(parts, AnyValueString(item))
		}
		return "[" + strings.Join(parts, ",") + "]"
	case *commonpb.AnyValue_KvlistValue:
		// Buraya yalnizca derinlik siniri asildiginda dusuluyor.
		if val.KvlistValue == nil {
			return "{}"
		}
		parts := make([]string, 0, len(val.KvlistValue.Values))
		for _, kv := range val.KvlistValue.Values {
			parts = append(parts, kv.GetKey()+"="+AnyValueString(kv.GetValue()))
		}
		return "{" + strings.Join(parts, ",") + "}"
	default:
		return ""
	}
}

// merge: ikinci haritayi birincinin uzerine yazmadan birlestirir.
// Cakisma olursa BIRINCI kazanir - daha ozel olan bilgi (span'in kendi
// ozniteligi) daha genel olani (kaynak ozniteligi) ezmemeli.
func merge(specific, general map[string]string) map[string]string {
	if len(general) == 0 {
		return specific
	}
	out := make(map[string]string, len(specific)+len(general))
	for k, v := range general {
		out[k] = v
	}
	for k, v := range specific {
		out[k] = v
	}
	return out
}

// hexID: OTLP kimliklerini metne cevirir.
//
// OTLP trace_id ve span_id'yi HAM BAYT olarak tasir (16 ve 8 bayt);
// PulseMetrics onaltilik metin olarak saklar. Uzunluk yanlissa bos
// donuyoruz ve cagiran kaydi reddediyor: gecersiz bir trace_id ile
// yazilan span, hicbir zaman bulunamayacak bir span demektir.
func hexID(b []byte, want int) string {
	if len(b) != want {
		return ""
	}
	// Tamami sifir olan kimlik, "yok" demektir (W3C boyle tanimlar).
	allZero := true
	for _, c := range b {
		if c != 0 {
			allZero = false
			break
		}
	}
	if allZero {
		return ""
	}
	return hex.EncodeToString(b)
}
