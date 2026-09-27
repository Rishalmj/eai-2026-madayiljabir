import { proxyActivities, ActivityFailure, ApplicationFailure } from "@temporalio/workflow";
import type * as activities from "../activities";
import type { CanonicalOrder, CheckoutResult, TraceItem } from "../types";

const act = proxyActivities<typeof activities>({
  startToCloseTimeout: "2500 ms",
  retry: {
    maximumAttempts: 3,
    initialInterval: "200 ms",
    backoffCoefficient: 2,
  },
});

function toCents(decimalString: string): number {
  return Math.round(parseFloat(decimalString) * 100);
}

function sumAmount(order: CanonicalOrder): string {
  const totalCents = order.items.reduce(
    (sum, item) => sum + toCents(item.unitPrice) * item.quantity,
    0,
  );
  return (totalCents / 100).toFixed(2);
}

/** What an ActivityFailure actually means: a code, and whether it timed out. */
function classify(err: unknown): { code: string; timedOut: boolean } {
  if (err instanceof ActivityFailure) {
    const cause = err.cause;
    if (cause && cause.name === "TimeoutFailure") {
      return { code: "timeout", timedOut: true };
    }
    if (cause instanceof ApplicationFailure) {
      return { code: cause.type ?? "unknown_error", timedOut: false };
    }
  }
  return { code: "unknown_error", timedOut: false };
}

async function runStep(
  step: string,
  trace: TraceItem[],
  fn: () => Promise<void>,
): Promise<{ ok: boolean; code?: string }> {
  const startedAt = new Date().toISOString();
  try {
    await fn();
    const finishedAt = new Date().toISOString();
    trace.push({
      step,
      status: "success",
      startedAt,
      finishedAt,
      durationMs: Date.parse(finishedAt) - Date.parse(startedAt),
    });
    return { ok: true };
  } catch (err) {
    const { code, timedOut } = classify(err);
    const finishedAt = new Date().toISOString();
    trace.push({
      step,
      status: timedOut ? "timeout" : "failed",
      startedAt,
      finishedAt,
      durationMs: Date.parse(finishedAt) - Date.parse(startedAt),
    });
    return { ok: false, code };
  }
}

export async function checkout(order: CanonicalOrder): Promise<CheckoutResult> {
  const trace: TraceItem[] = [];
  const amount = sumAmount(order);

  const payment = await runStep("payment", trace, () =>
    act.authorizePayment(order.orderId, amount),
  );
  if (!payment.ok) {
    return { orderId: order.orderId, status: "failed", code: payment.code, trace };
  }

  const inventory = await runStep("inventory", trace, () =>
    act.reserveInventory(order.orderId, order.items),
  );
  if (!inventory.ok) {
    await runStep("payment_refund", trace, () => act.refundPayment(order.orderId));
    return { orderId: order.orderId, status: "compensated", code: inventory.code, trace };
  }

  const shipping = await runStep("shipping", trace, () => act.createShipment(order.orderId));
  if (!shipping.ok) {
    await runStep("inventory_release", trace, () => act.releaseInventory(order.orderId));
    await runStep("payment_refund", trace, () => act.refundPayment(order.orderId));
    return { orderId: order.orderId, status: "compensated", code: shipping.code, trace };
  }

  const notification = await runStep("notification", trace, () =>
    act.sendNotification(order.orderId, order.customer.email),
  );
  if (!notification.ok) {
    await runStep("inventory_release", trace, () => act.releaseInventory(order.orderId));
    await runStep("payment_refund", trace, () => act.refundPayment(order.orderId));
    return { orderId: order.orderId, status: "compensated", code: notification.code, trace };
  }

  return { orderId: order.orderId, status: "completed", trace };
}