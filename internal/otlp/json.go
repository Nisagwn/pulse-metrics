package otlp

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
)

// idFields: OTLP semasinda "bytes" olan ve JSON'da HEX yazilan alanlar.
var idFields = map[string]int{
	"traceId":        16,
	"trace_id":       16,
	"spanId":         8,
	"span_id":        8,
	"parentSpanId":   8,
	"parent_span_id": 8,
}

// normalizeJSONIDs: OTLP/JSON'daki onaltilik kimlikleri base64'e cevirir.
//
// # NEDEN GEREKLI?
//
// Burada iki standart birbiriyle celisiyor.
//
// Protobuf'un resmi JSON eslemesi "bytes" alanlarini BASE64 olarak
// kodlar; protojson da bunu bekler. Ama OTLP/JSON belirtimi trace_id ve
// span_id icin ozellikle sapma yapiyor ve ONALTILIK metin sart kosuyor -
// cunku hata ayiklarken bir trace kimligini gozle okuyabilmek,
// standartla tutarlilik kaygisindan daha degerli.
//
// Sonuc: gercek bir OTel SDK'sinin gonderdigi JSON'u protojson'a
// dogrudan verirsek, 32 karakterlik onaltilik metni base64 sanip 24
// bayta cozer, uzunluk tutmaz ve span sessizce reddedilir. Kendi curl
// testin base64 ile calisirken gercek istemcilerin calismamasi - fark
// edilmesi en zor hata turlerinden.
//
// Cozum: JSON'u protojson'a vermeden once dolasip bu alanlari cevirmek.
// Zaten base64 olan degerler oldugu gibi birakiliyor, boylece ikisi de
// kabul ediliyor.
func normalizeJSONIDs(data []byte) ([]byte, error) {
	var doc interface{}
	if err := json.Unmarshal(data, &doc); err != nil {
		// Bozuk JSON'u burada raporlamiyoruz; protojson daha iyi bir
		// hata mesaji uretecek.
		return data, nil
	}
	if !walkIDs(doc) {
		// Hicbir sey degismedi: gereksiz yere yeniden kodlama.
		return data, nil
	}
	return json.Marshal(doc)
}

// walkIDs: belgeyi dolasip kimlik alanlarini cevirir.
// Bir sey degistiyse true doner.
func walkIDs(node interface{}) bool {
	changed := false
	switch v := node.(type) {
	case map[string]interface{}:
		for key, val := range v {
			if want, ok := idFields[key]; ok {
				if s, isStr := val.(string); isStr {
					if converted, did := hexToBase64(s, want); did {
						v[key] = converted
						changed = true
						continue
					}
				}
			}
			if walkIDs(val) {
				changed = true
			}
		}
	case []interface{}:
		for _, item := range v {
			if walkIDs(item) {
				changed = true
			}
		}
	}
	return changed
}

// hexToBase64: onaltilik bir kimligi base64'e cevirir.
// Onaltilik degilse (yani zaten base64 ise) dokunmadan gecer.
func hexToBase64(s string, wantBytes int) (string, bool) {
	if len(s) != wantBytes*2 {
		return s, false
	}
	raw, err := hex.DecodeString(s)
	if err != nil {
		return s, false
	}
	return base64.StdEncoding.EncodeToString(raw), true
}
