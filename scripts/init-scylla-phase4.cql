-- PulseMetrics Faz 4 semasi: paylasilan alarm durumu.
-- Collector acilista bunun aynisini kod icinden de calistirir.

-- ---------------------------------------------------------------------------
-- alert_state: hangi kural SU ANDA tetiklenmis?
--
-- Faz 3'te bu bilgi her collector'in kendi belleginde bir map[string]bool
-- idi. Tek collector varken dogru calisiyordu. Ikinci bir collector
-- acildigi anda bozuluyordu: iki surecin de kendi map'i vardi, ikisi de
-- ayni ihlali "yeni" sanip webhook gonderiyordu.
--
-- Dikkat: yatay olceklendirme sistemi CALISMAZ hale getirmiyordu - daha
-- sinsi bir sey yapiyordu, her alarmi iki kez bildiriyordu. Sessizce
-- yanlis davranan sistemler, gurultuyle bozulanlardan daha tehlikelidir.
--
-- Gecis LWT ile yapilir (bkz. internal/collector/alerts.go, tryTransition):
--
--   UPDATE alert_state SET firing = true, ...
--   WHERE rule_id = ? IF firing = false;
--
-- IF olmadan bu sradan bir UPDATE olurdu: "son yazan kazanir" mantigiyla
-- iki collector de basarili olur ve iki bildirim giderdi. IF ile Scylla
-- arka planda Paxos calistirir ve guncellemeyi SADECE mevcut deger
-- bekledigimiz degerse uygular. Yaristan tek kazanan cikar.
--
-- TTL YOK VE OLMAMALI. Bu tablo veri degil, KARAR tutuyor. Bir alarmin
-- tetiklenmis oldugu bilgisi 7 gun sonra kendiliginden silinirse, alarm
-- hala devam ederken sistem onu "yeni" sanip tekrar bildirir.
--
-- Partition basina tek satir dustugu icin tablo dogal olarak sinirli:
-- kural sayisi kadar partition, her birinde bir satir.
-- ---------------------------------------------------------------------------
-- notified kolonu: gecis ile bildirim ATOMIK DEGIL.
--
-- Gecisi kazanan surec, alarmi veritabanina yazip webhook'u gondermeden
-- once olebilir. O zaman satir "tetiklenmis" der, ihlal surer ve hicbir
-- degerlendirme bir daha bildirim uretmez - alarm sonsuza kadar susar.
--
-- Cozum satiri kucuk bir is kuyrugu gibi kullanmak: notified=false
-- "yarim kalmis is" demektir. Sahibi makul bir sure icinde (iki
-- degerlendirme turu) bitirmediyse baska bir collector devralir.
CREATE TABLE IF NOT EXISTS pulse.alert_state (
    rule_id    TEXT PRIMARY KEY,
    firing     BOOLEAN,
    notified   BOOLEAN,  -- bildirim tamamlandi mi?
    since_ms   BIGINT,   -- bu duruma ne zaman gecildi
    owner      TEXT,     -- gecisi hangi collector yapti (hata ayiklama icin)
    updated_ms BIGINT
);

-- Faz 4'ten kalan tablolar icin: SIRADAN bir kolon eklemek ucuz ve
-- cevrimici bir islem - Scylla yalnizca sema surumunu gunceller, veriye
-- dokunmaz. Partition key degistirmenin butun veriyi tasimayi
-- gerektirdigini (bkz. cmd/pulse-migrate) dusununce, aradaki fark
-- carpici.
--
-- ALTER'in IF NOT EXISTS karsiligi yok; kolon zaten varsa hata doner ve
-- collector bu hatayi yutar.
ALTER TABLE pulse.alert_state ADD notified BOOLEAN;
