import asyncio
import os

import aio_pika


RABBITMQ_URL = os.getenv("RABBITMQ_URL")


async def main() -> None:
    connection = await aio_pika.connect_robust(RABBITMQ_URL)

    async with connection:
        channel = await connection.channel()

        await channel.set_qos(prefetch_count=1)

        queue_products   = await channel.get_queue("products")
        queue_persist_db = await channel.get_queue("q.db")
        queue_persist_es = await channel.get_queue("q.es")

        print(" [*] Orchestrator started. Waiting for messages...")

        async with queue_products.iterator() as queue_iter:
            async for message in queue_iter:
                async with message.process():
                    print(f" [x] Received message from products: {message.body.decode()}")

                    # Republication persistante dans les deux queues cibles
                    persistent = aio_pika.DeliveryMode.PERSISTENT

                    await channel.default_exchange.publish(
                        aio_pika.Message(body=message.body, delivery_mode=persistent),
                        routing_key=queue_persist_db.name,
                    )

                    await channel.default_exchange.publish(
                        aio_pika.Message(body=message.body, delivery_mode=persistent),
                        routing_key=queue_persist_es.name,
                    )

                    print(" [>] Message duplicated to q.db and q.es")
                    # L'ack est géré automatiquement par le context manager message.process() (auto consume)


if __name__ == "__main__":
    asyncio.run(main())