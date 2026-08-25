import os
import sys
import json
import time
from typing import List, Dict, Any, Optional
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Proje dizinini sys.path'e ekle
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from flight_collector import FlightDataCollector

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")


class FlightKafkaProducer:
    """FlightDataCollector'dan gelen canlı uçuş verilerini serialize edip
    Apache Kafka Topic'ine güvenli ve performanslı bir şekilde basan Producer sınıfı.
    """

    def __init__(self, bootstrap_servers: str = "localhost:9092"):
        self.bootstrap_servers = bootstrap_servers
        self.collector = FlightDataCollector()
        self._init_producer()

    def _init_producer(self):
        """KafkaProducer nesnesini JSON serializer yapılandırması ile başlatır."""
        print(f"🔌 Kafka Broker'ına bağlanılıyor: {self.bootstrap_servers}...")
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                # Key'i string -> bytes çevirici
                key_serializer=lambda k: str(k).encode("utf-8") if k else None,
                # Value'yu dict -> JSON string -> bytes çevirici
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                # Gönderim hızını artırmak için batch ve compression ayarları
                acks=1,  # Lider broker'ın mesajı aldığını teyit etmesi yeterli
                retries=3,
                max_in_flight_requests_per_connection=5,
            )
            print("✅ Kafka Producer bağlantısı başarıyla kuruldu.")
        except Exception as e:
            print(f"❌ Kafka Producer başlatılamadı: {e}")
            raise e

    def publish_flights(self, flights: List[Dict[str, Any]], topic: str = "live-flights") -> Dict[str, Any]:
        """Verilen uçuş listesini Kafka topic'ine asenkron olarak basar ve flush eder.
        
        Args:
            flights: Normalize edilmiş uçuş sözlükleri listesi
            topic: Hedef Kafka Topic adı (varsayılan: 'live-flights')
            
        Returns:
            Özet işlem raporu
        """
        if not flights:
            print("⚠️ Gönderilecek uçuş verisi bulunamadı.")
            return {"status": "empty", "sent_count": 0}

        print(f"🚀 {len(flights)} uçuş verisi '{topic}' topic'ine gönderiliyor...")
        start_time = time.time()
        sent_count = 0
        failed_count = 0

        for flight in flights:
            # Key olarak uçuş ID veya flight_number kullan (partitioning için)
            msg_key = flight.get("flight_id") or flight.get("flight_number") or flight.get("callsign")
            try:
                self.producer.send(topic=topic, key=msg_key, value=flight)
                sent_count += 1
            except KafkaError as err:
                failed_count += 1
                print(f"❌ Mesaj gönderilemedi ({msg_key}): {err}")

        # Tüm mesajların broker'a ulaştığından emin olmak için flush et
        self.producer.flush()
        elapsed = round(time.time() - start_time, 2)

        print(f"✨ Başarıyla Gönderildi: {sent_count} mesaj | Başarısız: {failed_count} | Süre: {elapsed} sn")
        return {
            "status": "success",
            "topic": topic,
            "total_records": len(flights),
            "sent_count": sent_count,
            "failed_count": failed_count,
            "elapsed_seconds": elapsed
        }

    def collect_and_publish(self, target_count: int = 1200, topic: str = "live-flights") -> Dict[str, Any]:
        """FlightRadar'dan canlı verileri toplayıp doğrudan Kafka'ya gönderen uçtan uca metod."""
        print(f"\n========================================================")
        print(f"🛫 FLIGHTRADAR ➔ KAFKA VERİ AKIŞI BAŞLATILIYOR (Hedef: {target_count})")
        print(f"========================================================")
        
        # 1. Adım: Verileri topla
        flights = self.collector.fetch_bulk_flights(target_count=target_count)
        
        if not flights:
            return {"status": "error", "message": "FlightRadar'dan veri alınamadı."}

        # 2. Adım: Kafka'ya bas
        result = self.publish_flights(flights=flights, topic=topic)
        return result

    def close(self):
        """Producer bağlantısını güvenli kapatır."""
        if hasattr(self, "producer"):
            self.producer.close()
            print("🔒 Kafka Producer bağlantısı kapatıldı.")


if __name__ == "__main__":
    producer = FlightKafkaProducer()
    try:
        report = producer.collect_and_publish(target_count=1200, topic="live-flights")
        print("\n--- İŞLEM RAPORU ---")
        print(json.dumps(report, indent=2, ensure_ascii=False))
    finally:
        producer.close()
