"""RabbitMQ consumer for the `agendamento-script` exchange."""

from __future__ import annotations

import json
import logging
import signal
from typing import Any

import pika
from pika.adapters.blocking_connection import BlockingChannel, BlockingConnection

RABBITMQ_HOST = "localhost"
RABBITMQ_PORT = 5672
RABBITMQ_VHOST = "/"
RABBITMQ_USER = "guest"
RABBITMQ_PASSWORD = "guest"
RABBITMQ_HEARTBEAT = 60
RABBITMQ_PREFETCH = 1

EXCHANGE_NAME = "messaging"
EXCHANGE_TYPE = "direct"
QUEUE_NAME = "messaging-queue"
ROUTING_KEY = "messaging.execute"

RESPONSE_EXCHANGE_NAME = "consumer"
RESPONSE_EXCHANGE_TYPE = "direct"
RESPONSE_QUEUE_NAME = "consumer-queue"
RESPONSE_ROUTING_KEY = "consumer.execute"


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


def _connection_parameters() -> pika.connection.Parameters:
    """Build connection parameters based on the hard-coded RabbitMQ settings."""
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
    return pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        virtual_host=RABBITMQ_VHOST,
        credentials=credentials,
        heartbeat=RABBITMQ_HEARTBEAT,
    )


def _declare_infrastructure(channel: BlockingChannel) -> None:
    """Ensure all exchanges and queues exist before consuming messages."""
    channel.exchange_declare(
        exchange=EXCHANGE_NAME,
        exchange_type=EXCHANGE_TYPE,
        durable=True,
        passive=False,
    )
    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    channel.queue_bind(
        queue=QUEUE_NAME,
        exchange=EXCHANGE_NAME,
        routing_key=ROUTING_KEY,
    )

    channel.exchange_declare(
        exchange=RESPONSE_EXCHANGE_NAME,
        exchange_type=RESPONSE_EXCHANGE_TYPE,
        durable=True,
        passive=False,
    )
    channel.queue_declare(queue=RESPONSE_QUEUE_NAME, durable=True)
    channel.queue_bind(
        queue=RESPONSE_QUEUE_NAME,
        exchange=RESPONSE_EXCHANGE_NAME,
        routing_key=RESPONSE_ROUTING_KEY,
    )


def process_message(payload: Any) -> None:
    """
    Handle the message body (placeholder for future business logic) and log it.
    """
    logging.info("Processed payload: %s", payload)


def _parse_body(body: bytes) -> Any:
    """Parse message body as JSON when possible, otherwise return raw text."""
    try:
        return json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return body.decode("utf-8", errors="replace")


def _forward_to_backup(channel: BlockingChannel, payload: Any) -> None:
    """Serialize the payload as JSON and send it to the backup exchange/queue."""
    serialized = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    channel.basic_publish(
        exchange=RESPONSE_EXCHANGE_NAME,
        routing_key=RESPONSE_ROUTING_KEY,
        body=serialized,
        properties=pika.BasicProperties(
            delivery_mode=2,
            content_type="application/json",
        ),
    )


def _on_message(channel: BlockingChannel, method, properties, body: bytes) -> None:
    """
    Callback invoked for every message pulled from the queue.

    Acks the message when processing succeeds, otherwise requeues.
    """
    try:
        payload = _parse_body(body)
        process_message(payload)
        _forward_to_backup(channel, payload)
        channel.basic_ack(delivery_tag=method.delivery_tag)
    except Exception:  # pragma: no cover - defensive guard
        logging.exception("Failed to process message, requeuing")
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)


def start_consumer() -> None:
    """Start the blocking RabbitMQ consumer."""
    connection = BlockingConnection(_connection_parameters())
    channel = connection.channel()
    _declare_infrastructure(channel)

    channel.basic_qos(prefetch_count=RABBITMQ_PREFETCH)
    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=_on_message)

    def _graceful_shutdown(signum, frame) -> None:
        logging.info("Signal %s received. Stopping consumer...", signum)
        if channel.is_open:
            channel.stop_consuming()

    signal.signal(signal.SIGINT, _graceful_shutdown)
    signal.signal(signal.SIGTERM, _graceful_shutdown)

    logging.info(
        "Connecting to RabbitMQ at %s:%s (vhost '%s')",
        RABBITMQ_HOST,
        RABBITMQ_PORT,
        RABBITMQ_VHOST,
    )
    logging.info(
        "Listening on exchange '%s' / queue '%s' with routing key '%s'",
        EXCHANGE_NAME,
        QUEUE_NAME,
        ROUTING_KEY,
    )

    try:
        channel.start_consuming()
    finally:
        if not connection.is_closed:
            connection.close()
        logging.info("RabbitMQ consumer stopped")


if __name__ == "__main__":
    start_consumer()
