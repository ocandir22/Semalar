"""
Centralized MCP Request Audit Logger.
Intercepts all MCP tool executions (from AI Agents and HTTP RPC),
records metrics to the Kafka topic 'mcp-requests', and maintains
an in-memory circular buffer for the real-time Cockpit UI.
"""

import json
import time
import collections
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from kafka import KafkaProducer

_recent_audit_logs = collections.deque(maxlen=100)
_audit_producer: Optional[KafkaProducer] = None
_producer_failed = False


def get_audit_producer() -> Optional[KafkaProducer]:
    """Lazily initializes and caches the Kafka producer for MCP audit logs."""
    global _audit_producer, _producer_failed
    if _audit_producer is not None:
        return _audit_producer
    if _producer_failed:
        return None

    try:
        _audit_producer = KafkaProducer(
            bootstrap_servers="localhost:9092",
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
            key_serializer=lambda k: str(k).encode("utf-8") if k else None,
            acks=0,
            retries=1,
            request_timeout_ms=2000
        )
        return _audit_producer
    except Exception:
        _producer_failed = True
        return None


def log_mcp_request(tool_name: str, args: dict, result: Any, elapsed_ms: float):
    """
    Logs an MCP tool execution:
    1. Appends structured entry to in-memory ring buffer (for `/api/kafka/logs`).
    2. Emits real-time event to Apache Kafka topic 'mcp-requests'.
    3. Prints human-readable audit log to terminal.
    """
    try:
        status = "success"
        matched_count = None
        if isinstance(result, dict):
            status = result.get("status", "success")
            if "total_matches" in result:
                matched_count = result["total_matches"]
            elif "returned_count" in result:
                matched_count = result["returned_count"]
            elif "flights" in result and isinstance(result["flights"], list):
                matched_count = len(result["flights"])
            elif "total_tracked" in result:
                matched_count = result["total_tracked"]
            elif "total_flights_found" in result:
                matched_count = result["total_flights_found"]

        payload = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "tool_name": tool_name,
            "arguments": args or {},
            "status": status,
            "matched_records": matched_count,
            "execution_time_ms": round(elapsed_ms, 2)
        }
        _recent_audit_logs.appendleft(payload)

        # Publish to Kafka 'mcp-requests' topic
        producer = get_audit_producer()
        if producer:
            try:
                producer.send("mcp-requests", key=tool_name, value=payload)
            except Exception:
                pass

        # Terminal Visual Output
        active_args = {k: v for k, v in (args or {}).items() if v not in [None, "", False] and not k.startswith("_")}
        args_repr = ", ".join(f"{k}={repr(v)}" for k, v in active_args.items()) if active_args else "no args"
        records_info = f" | {matched_count} records" if matched_count is not None else ""
        print(f"  📡 \033[95m[KAFKA AUDIT]\033[0m \033[1m{tool_name}\033[0m({args_repr}) ➔ \033[92m{status}\033[0m ({elapsed_ms:.1f}ms{records_info}) ➔ Topic \033[96m'mcp-requests'\033[0m")
    except Exception:
        pass


def get_recent_audit_logs() -> List[Dict[str, Any]]:
    """Returns the most recent 100 MCP request audit log entries."""
    return list(_recent_audit_logs)
