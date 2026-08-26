"""
Flight Agent Compatibility Bridge — Provides backward compatibility for CLI and legacy scripts.
Routes queries directly to either Project #1 (Live Radar) or Project #2 (Kafka Telemetry).
"""

from typing import Dict, Any, Optional
from core.llm_client import get_agent_info
from project_live.live_agent import ask_live_agent, LIVE_MCP_DEFINITIONS
from project_kafka.kafka_agent import ask_kafka_agent, KAFKA_MCP_DEFINITIONS


async def ask_flight_agent(user_query: str, project_mode: str = "live", mcp_url: Optional[str] = None) -> Dict[str, Any]:
    """Routes user queries to the dedicated project agent."""
    mode = str(project_mode).lower().strip()
    if mode == "kafka":
        return await ask_kafka_agent(user_query)
    else:
        return await ask_live_agent(user_query)
