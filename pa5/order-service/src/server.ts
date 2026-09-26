/**
 * PA5 order-service — HTTP entry point, and DLQ replay.
 */

import crypto from "node:crypto";
import express from "express";
import { connectWithRetry, withoutRetryHistory } from "../../shared/rabbit";
import type { CanonicalOrder } from "../../shared/canonical-order";

const PORT = process.env.PORT ? Number(process.env.PORT) : 3000;
const EXCHANGE = "orders.exchange";
const RETURN_EXCHANGE = "orders.return.exchange";
const DLQ_EXCHANGE = process.env.DLQ_EXCHANGE ?? "orders.dlq.exchange";

// In-memory order store, keyed by correlationId.
const orders = new Map<string, CanonicalOrder>();

async function main(): Promise<void> {
  const { channel } = await connectWithRetry(process.env.RABBITMQ_URL!);

  // Declare the dead letter channel — replay reads from orders.dlq, and
  // channel.get on a queue nobody has declared yet closes the channel with
  // NOT_FOUND.
  await channel.assertExchange(DLQ_EXCHANGE, "fanout", { durable: true });
  await channel.assertQueue("orders.dlq", { durable: true });
  await channel.bindQueue("orders.dlq", DLQ_EXCHANGE, "");

  const app = express();
  app.use(express.json());

  app.post("/orders", (req, res) => {
    const order = req.body as CanonicalOrder;
    const correlationId = crypto.randomUUID();

    orders.set(correlationId, order);

    // Republish the order body UNCHANGED — correlationId lives only in the
    // AMQP header, never as a field on the order object (the canonical
    // schema's additionalProperties: false would reject it).
    channel.publish(EXCHANGE, "", Buffer.from(JSON.stringify(order)), {
      headers: { correlationId },
      contentType: "application/json",
      persistent: true,
    });

    res.status(201).json({ correlationId, status: "accepted" });
  });

  app.get("/orders/:correlationId", (req, res) => {
    const order = orders.get(req.params.correlationId);
    if (!order) {
      res.status(404).json({ error: "not found" });
      return;
    }
    res.status(200).json({ correlationId: req.params.correlationId, order });
  });

  app.post("/dlq/replay", async (_req, res) => {
    let replayed = 0;

    // Drain orders.dlq one message at a time, sending each back to the
    // consumer queue it originally failed out of.
    for (;;) {
      const msg = await channel.get("orders.dlq");
      if (!msg) break;

      const originQueue = msg.properties.headers?.["originQueue"] as string | undefined;
      if (!originQueue) {
        // No origin recorded — nothing sensible to replay this to. Ack it
        // rather than leaving it stuck looping through get().
        channel.ack(msg);
        continue;
      }

      channel.publish(RETURN_EXCHANGE, originQueue, msg.content, {
        // Drops x-death so the replayed message gets a fresh three attempts
        // instead of landing straight back in the DLQ.
        headers: withoutRetryHistory(msg.properties.headers),
        contentType: "application/json",
        persistent: true,
      });
      channel.ack(msg);
      replayed += 1;
    }

    res.status(200).json({ replayed });
  });

  app.get("/health", (_req, res) => {
    res.status(200).json({ status: "ok" });
  });

  app.listen(PORT, () => {
    console.log(`[order-service] listening on :${PORT}`);
  });
}

main().catch(console.error);