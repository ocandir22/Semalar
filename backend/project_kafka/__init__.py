"""
Project Kafka — Real-Time Apache Kafka Flight Telemetry Stream & Cockpit Analytics.
"""
from .flight_collector import FlightDataCollector
from .flight_producer import FlightKafkaProducer
from .flight_kafka_store import kafka_store, query_kafka_stream
from .kafka_agent import ask_kafka_agent, get_dynamic_mcp_tools
