import json
import logging
import os
from contextlib import asynccontextmanager

from aiokafka import AIOKafkaProducer
from fastapi import FastAPI, HTTPException

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Kafka settings from environment variables (set in docker-compose.yml)
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "test-topic")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")

producer: AIOKafkaProducer | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global producer
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode(),
        max_retries=3,
    )
    await producer.start()

    print(
        f"Kafka producer started for topic '{KAFKA_TOPIC}' at {KAFKA_BOOTSTRAP_SERVERS}"
    )

    yield  # The application runs here

    if producer:
        await producer.stop()
        print("Kafka producer stopped.")


app = FastAPI(lifespan=lifespan)


@app.get("/")
async def read_root():
    return {"message": "FastAPI with Kafka is running!"}


@app.post("/send-event/")
async def send_event(message: dict):
    """
    Endpoint to send a message to the Kafka topic.
    """
    global producer
    if not producer:
        raise HTTPException(
            status_code=500, detail="Kafka producer not available"
        )

    try:
        await producer.send(KAFKA_TOPIC, message)
        # Wait for the message to be acknowledged (optional, for production)
        # await producer.send_and_wait(KAFKA_TOPIC, message)
        return {"status": "Message sent successfully", "topic": KAFKA_TOPIC}
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to send message: {e}"
        )
