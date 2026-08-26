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

    def publish_turkey_flights(self, topic: str = "live-flights") -> Dict[str, Any]:
        """Fetches ALL active flights strictly within Turkish airspace (no limit) and publishes them to Kafka."""
        print(f"\n========================================================")
        print(f"🇹🇷 INITIATING TURKEY AIRSPACE ➔ KAFKA PIPELINE (ALL FLIGHTS)")
        print(f"========================================================")
        
        flights = self.collector.fetch_all_turkey_flights()
        if not flights:
            return {"status": "error", "message": "Failed to fetch Turkey flight data from FlightRadar."}

        return self.publish_flights(flights=flights, topic=topic)

    def stream_turkey_flights(self, interval_seconds: int = 15, topic: str = "live-flights", callback=None, stop_event=None):
        """Streams ALL live flights in Turkish airspace to Kafka continuously at fixed intervals."""
        print(f"\n========================================================")
        print(f"🇹🇷 REAL-TIME TURKEY AIRSPACE KAFKA STREAM INITIATED")
        print(f"📡 Refresh Interval : {interval_seconds}s | Target Topic: '{topic}'")
        print(f"⚡ Collecting 100% of aircraft across Turkey (no artificial cap)")
        print(f"========================================================")
        
        cycle = 1
        try:
            while not (stop_event and stop_event.is_set()):
                print(f"\n⏰ [Cycle #{cycle}] Fetching 100% of live aircraft over Turkey...")
                flights = self.collector.fetch_all_turkey_flights()
                if flights:
                    self.publish_flights(flights=flights, topic=topic)
                    if callback:
                        try:
                            callback(flights)
                        except Exception as cb_err:
                            print(f"⚠️ Callback error: {cb_err}")
                else:
                    print("⚠️ No flights returned from API in this cycle.")
                cycle += 1
                if stop_event:
                    if stop_event.wait(interval_seconds):
                        break
                else:
                    time.sleep(interval_seconds)
        except KeyboardInterrupt:
            print("\n🛑 Continuous Turkey flight stream stopped by user.")

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
    import argparse
    parser = argparse.ArgumentParser(description="Semalar FlightRadar24 ➔ Apache Kafka Live Telemetry Producer")
    parser.add_argument("--stream", action="store_true", help="Continuously stream ALL Turkey flights to Kafka in real time")
    parser.add_argument("--interval", type=int, default=15, help="Streaming refresh interval in seconds (default: 15)")
    parser.add_argument("--all-world", action="store_true", help="Fallback: collect 1200 worldwide flights")
    args = parser.parse_args()

    producer = FlightKafkaProducer()
    try:
        if args.stream:
            producer.stream_turkey_flights(interval_seconds=args.interval)
        elif args.all_world:
            report = producer.collect_and_publish(target_count=1200, topic="live-flights")
            print("\n--- EXECUTION REPORT ---")
            print(json.dumps(report, indent=2, ensure_ascii=False))
        else:
            report = producer.publish_turkey_flights(topic="live-flights")
            print("\n--- EXECUTION REPORT ---")
            print(json.dumps(report, indent=2, ensure_ascii=False))
    finally:
        producer.close()
