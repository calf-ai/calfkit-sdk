"""Lightweight HTTP-to-Kafka bridge.

Exposes a POST endpoint that publishes messages to Kafka topics,
used by the chat frontend to dispatch requests to agent nodes.

Usage:
    uv run uvicorn examples.chat.kafka_bridge:app --host 0.0.0.0 --port 8000
"""

import os

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from faststream.kafka.fastapi import KafkaRouter

router = KafkaRouter(os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))

app = FastAPI(title="Kafka HTTP Bridge")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(router)


BASE_SYSTEM_PROMPT = (
    "You are a high volume day trader. Your goal is to maximize your account value. "
    "You have access to tools that allow you to view your portfolio, and make trades. "
    "You will be invoked roughly every 5-10 seconds--at which time you can use your "
    "tools to view your portfolio and make trades, or if you decide not to, you can "
    "simply respond with a message explaining why not. "
    "Here are some more instructions: "
)


@app.post("/publish/{topic}")
async def publish(topic: str, message: dict):
    if "system_prompt" in message:
        message["system_prompt"] = BASE_SYSTEM_PROMPT + message["system_prompt"]
    await router.broker.publish(message, topic)
    return {"status": "ok", "topic": topic}


@app.get("/health")
async def health():
    return {"status": "healthy"}
