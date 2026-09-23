"""
Aggregator Service
==================
Your job: implement the AGGREGATOR pattern on top of the connection
handling and consume loop already wired up below.

- Collect item results from orders.results, grouped by orderId.
- Completion condition: every item of the order has reported a result.
- Timeout: if the order has been sitting incomplete for too long (one
  worker crashed, or was never running), emit a PARTIAL result instead of
  waiting forever. A hung order is a worse outcome than an honest partial
  answer.
- Duplicate results (the same item redelivered, e.g. after a requeue) must
  not be double-counted.
- Two orders in flight at once must never have their results mixed up.

Consumes from: orders.results
Publishes to:  orders.complete

Required shape of the message you publish to orders.complete (tests depend
on every one of these fields):

    {
        "orderId": "<the order this result set belongs to>",
        "correlationId": "<same value as orderId>",
        "status": "complete" | "partial",
        "totalItems": <the totalItems every item message carried>,
        "receivedItems": <how many distinct items you actually collected>,
        "itemResults": [ <the result messages you received, in any order> ],
        "missingItemIndexes": [ <itemIndex values you never received>
                                 -- empty list when status == "complete" ]
    }

Publish exactly ONE message to orders.complete per order, whichever status
it ends up with.
"""

import json
import pika
import os
import threading
import time


# Tracks every order that has at least one result in but is not yet
# complete. Keyed by orderId. Each value is a dict:
#   {
#       "correlationId": str,
#       "totalItems": int,
#       "results": { itemIndex: result_dict, ... },   # keyed so a
#                                                        # redelivered
#                                                        # duplicate just
#                                                        # overwrites the
#                                                        # same key instead
#                                                        # of adding a
#                                                        # second entry
#       "lastActivity": float,   # time.time() of the last result received
#   }
in_flight = {}
lock = threading.Lock()

# How long an order may sit with no new results before we give up and emit
# a partial result instead of waiting forever.
IDLE_TIMEOUT_SECONDS = float(os.environ.get('AGGREGATOR_IDLE_TIMEOUT_SECONDS', '5'))

# How often the background sweep checks for timed-out orders. Independent
# of IDLE_TIMEOUT_SECONDS; this just controls how promptly a timeout is
# noticed once it has actually elapsed.
SWEEP_INTERVAL_SECONDS = 1.0


def get_rabbitmq_connection():
    """Create a connection to RabbitMQ using environment variable for host."""
    return pika.BlockingConnection(
        pika.ConnectionParameters(host=os.environ.get('RABBITMQ_HOST', 'localhost'))
    )


def publish_completion(message):
    """Publish a single message to orders.complete. Called with the lock
    already released -- do not hold `lock` while doing network I/O."""
    connection = get_rabbitmq_connection()
    channel = connection.channel()
    channel.queue_declare(queue='orders.complete', durable=True)
    channel.basic_publish(
        exchange='',
        routing_key='orders.complete',
        body=json.dumps(message),
        properties=pika.BasicProperties(delivery_mode=2)  # Persistent
    )
    connection.close()


def _build_message(order_id, entry, status):
    """Build the orders.complete message shape from an in-flight entry."""
    total_items = entry['totalItems']
    received_indexes = set(entry['results'].keys())
    missing = sorted(i for i in range(total_items) if i not in received_indexes)

    return {
        "orderId": order_id,
        "correlationId": entry['correlationId'],
        "status": status,
        "totalItems": total_items,
        "receivedItems": len(entry['results']),
        "itemResults": list(entry['results'].values()),
        "missingItemIndexes": missing,
    }


def aggregate_result(ch, method, properties, body):
    """
    Handle one message from orders.results.
    """
    try:
        result = json.loads(body)
        order_id = result['orderId']
        item_index = result['itemIndex']
        total_items = result['totalItems']
        correlation_id = result.get('correlationId', order_id)
    except (json.JSONDecodeError, KeyError) as err:
        # A message we cannot even parse enough to know which order it
        # belongs to is not something we can safely retry -- log it and
        # move on rather than jamming the queue.
        print(f"[Aggregator] WARNING: dropping unparseable result: {err}")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return

    message_to_publish = None

    with lock:
        entry = in_flight.setdefault(order_id, {
            "correlationId": correlation_id,
            "totalItems": total_items,
            "results": {},
            "lastActivity": time.time(),
        })

        # Keyed by itemIndex: a redelivered duplicate overwrites the same
        # key instead of being counted as a second, distinct item.
        entry['results'][item_index] = result
        entry['lastActivity'] = time.time()

        if len(entry['results']) >= entry['totalItems']:
            message_to_publish = _build_message(order_id, entry, "complete")
            del in_flight[order_id]

    # Publish outside the lock -- network I/O should never happen while
    # holding it, or every other order's processing blocks on this one.
    if message_to_publish is not None:
        publish_completion(message_to_publish)
        print(f"[Aggregator] Order {order_id} complete")

    ch.basic_ack(delivery_tag=method.delivery_tag)


def sweep_timeouts():
    """
    Runs forever in a background thread, started from main(). Every
    SWEEP_INTERVAL_SECONDS, looks for orders that have gone quiet for
    longer than IDLE_TIMEOUT_SECONDS and emits a partial result for them.
    """
    while True:
        time.sleep(SWEEP_INTERVAL_SECONDS)

        now = time.time()
        messages_to_publish = []

        with lock:
            timed_out_ids = [
                order_id
                for order_id, entry in in_flight.items()
                if now - entry['lastActivity'] > IDLE_TIMEOUT_SECONDS
            ]

            for order_id in timed_out_ids:
                entry = in_flight[order_id]
                messages_to_publish.append(_build_message(order_id, entry, "partial"))
                del in_flight[order_id]

        for message in messages_to_publish:
            publish_completion(message)
            print(f"[Aggregator] Order {message['orderId']} timed out, "
                  f"published partial result (missing {message['missingItemIndexes']})")


def main():
    """Main entry point: connect to RabbitMQ, start the timeout sweeper,
    and start consuming results."""
    connection = get_rabbitmq_connection()
    channel = connection.channel()

    # Declare queues (idempotent)
    channel.queue_declare(queue='orders.results', durable=True)
    channel.queue_declare(queue='orders.complete', durable=True)

    # Fair dispatch
    channel.basic_qos(prefetch_count=1)

    # Background thread: sweeps for orders that timed out waiting on a
    # worker that never answered.
    sweeper = threading.Thread(target=sweep_timeouts, daemon=True)
    sweeper.start()

    # Start consuming
    channel.basic_consume(queue='orders.results', on_message_callback=aggregate_result)

    print('[Aggregator] Waiting for results...')
    channel.start_consuming()


if __name__ == '__main__':
    main()