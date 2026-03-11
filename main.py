import json
import logging
import os
from contextlib import asynccontextmanager

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from fastapi import FastAPI, HTTPException

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Kafka settings from environment variables (set in docker-compose.yml)
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "test-topic")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_GROUP_ID = "group-1"

producer: AIOKafkaProducer | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global producer
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode(),
    )
    await producer.start()

    logger.info(
        f"Kafka producer started for topic '{KAFKA_TOPIC}' at {KAFKA_BOOTSTRAP_SERVERS}"
    )

    yield  # The application runs here

    if producer:
        await producer.stop()
        logger.info("Kafka producer stopped.")


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


@app.get("/consume-messages/")
async def consume_messages(max_messages: int = 10, timeout_ms: int = 1000):
    """Endpoint to consume messages from Kafka"""
    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_GROUP_ID,
        value_deserializer=lambda m: json.loads(m.decode("ascii")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        max_poll_records=max_messages,
        max_poll_interval_ms=10000,  # 10 seconds
        session_timeout_ms=30000,  # 30 seconds
        heartbeat_interval_ms=10000,  # 10 seconds
    )

    await consumer.start()
    logger.info(
        f"Kafka consumer started for topic '{KAFKA_TOPIC}' at {KAFKA_BOOTSTRAP_SERVERS}"
    )

    try:
        messages = []

        # Poll with timeout
        batch = await consumer.getmany(
            timeout_ms=timeout_ms, max_records=max_messages
        )

        for tp, msgs in batch.items():
            for msg in msgs:
                messages.append(
                    {
                        "value": msg.value,
                        "topic": msg.topic,
                        "partition": msg.partition,
                        "offset": msg.offset,
                        "timestamp": msg.timestamp,
                    }
                )
                if len(messages) >= max_messages:
                    break

        return {"messages": messages, "count": len(messages)}

    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to consume messages: {e}"
        )

    finally:
        await consumer.stop()
