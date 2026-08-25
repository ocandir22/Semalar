import os
import sys
import json
import time
from typing import List, Dict, Any, Optional
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Ensure backend directory is in sys.path
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from flight_collector import FlightDataCollector

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")


class FlightKafkaProducer:
    """Producer class that serializes live flight telemetry from FlightDataCollector
    and publishes records reliably and performantly to an Apache Kafka Topic.
    """

    def __init__(self, bootstrap_servers: str = "localhost:9092"):
        self.bootstrap_servers = bootstrap_servers
        self.collector = FlightDataCollector()
        self._init_producer()

    def _init_producer(self):
        """Initializes KafkaProducer with JSON serialization configuration."""
        print(f"🔌 Connecting to Kafka broker: {self.bootstrap_servers}...")
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                key_serializer=lambda k: str(k).encode("utf-8") if k else None,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                acks=1,
                retries=3,
                max_in_flight_requests_per_connection=5,
            )
            print("✅ Kafka Producer connected successfully.")
        except Exception as e:
            print(f"❌ Failed to initialize Kafka Producer: {e}")
            raise e

    def publish_flights(self, flights: List[Dict[str, Any]], topic: str = "live-flights") -> Dict[str, Any]:
        """Publishes a list of normalized flight records to a Kafka topic and flushes buffer.
        
        Args:
            flights: List of normalized flight telemetry dictionaries
            topic: Target Kafka Topic name (default: 'live-flights')
            
        Returns:
            Summary execution report
        """
        if not flights:
            print("⚠️ No flight data provided to publish.")
            return {"status": "empty", "sent_count": 0}

        print(f"🚀 Publishing {len(flights)} flight records to topic '{topic}'...")
        start_time = time.time()
        sent_count = 0
        failed_count = 0

        for flight in flights:
            msg_key = flight.get("flight_id") or flight.get("flight_number") or flight.get("callsign")
            try:
                self.producer.send(topic=topic, key=msg_key, value=flight)
                sent_count += 1
            except KafkaError as err:
                failed_count += 1
                print(f"❌ Failed to send message ({msg_key}): {err}")

        # Flush to ensure all queued messages are sent
        self.producer.flush()
        elapsed = round(time.time() - start_time, 2)

        print(f"✨ Successfully published: {sent_count} messages | Failed: {failed_count} | Duration: {elapsed}s")
        return {
            "status": "success",
            "topic": topic,
            "total_records": len(flights),
            "sent_count": sent_count,
            "failed_count": failed_count,
            "elapsed_seconds": elapsed
        }

    def collect_and_publish(self, target_count: int = 1200, topic: str = "live-flights") -> Dict[str, Any]:
        """End-to-end pipeline fetching live data from FlightRadar24 and streaming directly to Kafka."""
        print(f"\n========================================================")
        print(f"🛫 INITIATING FLIGHTRADAR ➔ KAFKA PIPELINE (Target: {target_count})")
        print(f"========================================================")
        
        # Step 1: Collect raw flights
        flights = self.collector.fetch_bulk_flights(target_count=target_count)
        
        if not flights:
            return {"status": "error", "message": "Failed to fetch flight data from FlightRadar."}

        # Step 2: Publish to Kafka
        result = self.publish_flights(flights=flights, topic=topic)
        return result

    def close(self):
        """Safely closes Kafka Producer connection."""
        if hasattr(self, "producer"):
            self.producer.close()
            print("🔒 Kafka Producer connection closed.")


if __name__ == "__main__":
    producer = FlightKafkaProducer()
    try:
        report = producer.collect_and_publish(target_count=1200, topic="live-flights")
        print("\n--- EXECUTION REPORT ---")
        print(json.dumps(report, indent=2, ensure_ascii=False))
    finally:
        producer.close()
