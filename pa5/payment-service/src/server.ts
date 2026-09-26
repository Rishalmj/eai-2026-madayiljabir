/**
 * PA5 payment-service — the reference consumer. Connection, consume setup,
 * and the retry/DLQ logic are given below and work as-is. Three TODOs are
 * yours: declare the two channels this consumer publishes to, classify a
 * failure as permanent or temporary, and the payment simulation itself.
 * Study this file, then replicate the pattern in inventory-service and
 * notification-service.
 */

import type { ConsumeMessage } from "amqplib";
import { connectWithRetry, getRetryCount } from "../../shared/rabbit";
import type { CanonicalOrder } from "../../shared/canonical-order";

const QUEUE = "payments.queue";
const RESULTS_EXCHANGE = "results.payment";
const DLQ_EXCHANGE = process.env.DLQ_EXCHANGE ?? "orders.dlq.exchange";
const INVALID_EXCHANGE = "orders.invalid.exchange";
const MAX_RETRIES = Number(process.env.MAX_RETRIES ?? "3");
const FAIL_RATE = Number(process.env.PAYMENT_FAIL_RATE ?? "20");

async function main(): Promise<void> {
  const { channel } = await connectWithRetry(process.env.RABBITMQ_URL!);
  await channel.prefetch(1);

  // Declare the dead letter channel and the invalid message channel — same
  // exact properties everywhere this is declared (README.md table). A
  // mismatched second declaration is refused with PRECONDITION_FAILED.
  await channel.assertExchange(DLQ_EXCHANGE, "fanout", { durable: true });
  await channel.assertQueue("orders.dlq", { durable: true });
  await channel.bindQueue("orders.dlq", DLQ_EXCHANGE, "");

  await channel.assertExchange(INVALID_EXCHANGE, "fanout", { durable: true });
  await channel.assertQueue("orders.invalid", { durable: true });
  await channel.bindQueue("orders.invalid", INVALID_EXCHANGE, "");

  console.log(`[Payment] Consuming from ${QUEUE}, fail rate: ${FAIL_RATE}%`);

  await channel.consume(QUEUE, async (msg: ConsumeMessage | null) => {
    if (!msg) return;

    const correlationId = msg.properties.headers?.["correlationId"] as string | undefined;
    const retryCount = getRetryCount(msg);

    console.log(`[Payment] Processing ${correlationId} (attempt ${retryCount + 1})`);

    try {
      const order = JSON.parse(msg.content.toString()) as CanonicalOrder;

      const roll = Math.random() * 100;
      if (roll < FAIL_RATE) {
        throw new Error(`Simulated payment failure (roll ${roll.toFixed(1)} < ${FAIL_RATE})`);
      }

      channel.ack(msg);
      channel.publish(
        RESULTS_EXCHANGE,
        "",
        Buffer.from(
          JSON.stringify({
            correlationId,
            source: "payment",
            status: "success",
            timestamp: new Date().toISOString(),
            details: { message: `Payment approved for order ${order.orderId}` },
          }),
        ),
        {
          headers: { correlationId },
          contentType: "application/json",
          persistent: true,
        },
      );
      console.log(`[Payment] Success for ${correlationId}`);
    } catch (err) {
      // Classify before retrying. A body that is not valid JSON fails
      // identically on every attempt — a JSON.parse failure (SyntaxError)
      // is permanent: straight to the invalid message channel, original
      // bytes and headers, no retry.
      if (err instanceof SyntaxError) {
        channel.publish(INVALID_EXCHANGE, "", msg.content, {
          headers: { ...msg.properties.headers },
          persistent: true,
        });
        channel.ack(msg);
        console.log(`[Payment] -> Invalid (unparseable body): ${err.message}`);
        return;
      }

      // Retry / DLQ logic (provided — study this for the other two services).
      // Do not modify.
      if (retryCount >= MAX_RETRIES - 1) {
        channel.publish(DLQ_EXCHANGE, "", msg.content, {
          headers: { ...msg.properties.headers, originQueue: QUEUE },
          persistent: true,
        });
        channel.ack(msg);
        console.log(`[Payment] -> DLQ after ${retryCount + 1} attempts: ${(err as Error).message}`);
      } else {
        channel.nack(msg, false, false);
        console.log(`[Payment] -> Retry (attempt ${retryCount + 1}): ${(err as Error).message}`);
      }
    }
  });
}

main().catch(console.error);