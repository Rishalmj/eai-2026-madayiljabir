/**
 * PA5 notification-service — consumes notifications.queue, deduplicates by
 * correlationId using a file (survives a container restart, unlike an
 * in-memory Set — see docs/adr-004.md), and appends one JSON line per new
 * order to /data/notification.log.
 */

import { existsSync, mkdirSync, readFileSync, writeFileSync, appendFileSync } from "node:fs";
import path from "node:path";
import type { ConsumeMessage } from "amqplib";
import { connectWithRetry, getRetryCount } from "../../shared/rabbit";
import type { CanonicalOrder } from "../../shared/canonical-order";

const QUEUE = "notifications.queue";
const RESULTS_EXCHANGE = "results.notification";
const DLQ_EXCHANGE = process.env.DLQ_EXCHANGE ?? "orders.dlq.exchange";
const INVALID_EXCHANGE = "orders.invalid.exchange";
const MAX_RETRIES = Number(process.env.MAX_RETRIES ?? "3");
const FAIL_RATE = Number(process.env.NOTIFICATION_FAIL_RATE ?? "10");

const DATA_DIR = "/data";
const PROCESSED_IDS_PATH = path.join(DATA_DIR, "processed-ids.json");
const LOG_PATH = path.join(DATA_DIR, "notification.log");

function loadProcessedIds(): Set<string> {
  mkdirSync(DATA_DIR, { recursive: true });
  if (!existsSync(PROCESSED_IDS_PATH)) {
    writeFileSync(PROCESSED_IDS_PATH, "[]", "utf8");
    return new Set();
  }
  const raw = readFileSync(PROCESSED_IDS_PATH, "utf8");
  const ids = JSON.parse(raw) as string[];
  return new Set(ids);
}

function persistProcessedIds(ids: Set<string>): void {
  writeFileSync(PROCESSED_IDS_PATH, JSON.stringify([...ids]), "utf8");
}

async function main(): Promise<void> {
  const { channel } = await connectWithRetry(process.env.RABBITMQ_URL!);
  await channel.prefetch(1);

  await channel.assertExchange(DLQ_EXCHANGE, "fanout", { durable: true });
  await channel.assertQueue("orders.dlq", { durable: true });
  await channel.bindQueue("orders.dlq", DLQ_EXCHANGE, "");

  await channel.assertExchange(INVALID_EXCHANGE, "fanout", { durable: true });
  await channel.assertQueue("orders.invalid", { durable: true });
  await channel.bindQueue("orders.invalid", INVALID_EXCHANGE, "");

  const processedIds = loadProcessedIds();

  console.log(`[Notification] Consuming from ${QUEUE}, known duplicates so far: ${processedIds.size}`);

  await channel.consume(QUEUE, async (msg: ConsumeMessage | null) => {
    if (!msg) return;

    const correlationId = msg.properties.headers?.["correlationId"] as string | undefined;
    const retryCount = getRetryCount(msg);

    try {
      // Check duplicates BEFORE parsing — this must catch a redelivery of an
      // already-processed order even if nothing else about the message
      // needs inspecting.
      if (correlationId && processedIds.has(correlationId)) {
        channel.ack(msg);
        console.log(`[Notification] duplicate skipped: ${correlationId}`);
        return;
      }

      const order = JSON.parse(msg.content.toString()) as CanonicalOrder;

      const roll = Math.random() * 100;
      if (roll < FAIL_RATE) {
        throw new Error(`Simulated notification failure (roll ${roll.toFixed(1)} < ${FAIL_RATE})`);
      }

      const logLine = {
        correlationId,
        orderId: order.orderId,
        // NOTE: verify this path against shared/canonical-order.ts — adjust
        // if the email field lives somewhere else on CanonicalOrder.
        customerEmail: (order as any).customer?.email,
        timestamp: new Date().toISOString(),
        message: "Order received",
      };
      appendFileSync(LOG_PATH, JSON.stringify(logLine) + "\n", "utf8");

      if (correlationId) {
        processedIds.add(correlationId);
        persistProcessedIds(processedIds);
      }

      channel.ack(msg);
      channel.publish(
        RESULTS_EXCHANGE,
        "",
        Buffer.from(
          JSON.stringify({
            correlationId,
            source: "notification",
            status: "success",
            timestamp: new Date().toISOString(),
            details: { message: `Notification sent for order ${order.orderId}` },
          }),
        ),
        {
          headers: { correlationId },
          contentType: "application/json",
          persistent: true,
        },
      );
      console.log(`[Notification] Success for ${correlationId}`);
    } catch (err) {
      if (err instanceof SyntaxError) {
        channel.publish(INVALID_EXCHANGE, "", msg.content, {
          headers: { ...msg.properties.headers },
          persistent: true,
        });
        channel.ack(msg);
        console.log(`[Notification] -> Invalid (unparseable body): ${err.message}`);
        return;
      }

      if (retryCount >= MAX_RETRIES - 1) {
        channel.publish(DLQ_EXCHANGE, "", msg.content, {
          headers: { ...msg.properties.headers, originQueue: QUEUE },
          persistent: true,
        });
        channel.ack(msg);
        console.log(`[Notification] -> DLQ after ${retryCount + 1} attempts: ${(err as Error).message}`);
      } else {
        channel.nack(msg, false, false);
        console.log(`[Notification] -> Retry (attempt ${retryCount + 1}): ${(err as Error).message}`);
      }
    }
  });
}

main().catch(console.error);