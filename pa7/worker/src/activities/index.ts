import { ApplicationFailure } from "@temporalio/activity";
import axios from "axios";
import type { CanonicalItem } from "../types";

function requiredEnv(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`Missing required environment variable: ${name}`);
  return value;
}

const PAYMENT_URL = requiredEnv("PAYMENT_URL");
const INVENTORY_URL = requiredEnv("INVENTORY_URL");
const SHIPPING_URL = requiredEnv("SHIPPING_URL");
const NOTIFICATION_URL = requiredEnv("NOTIFICATION_URL");

/**
 * Makes the POST call and classifies any failure:
 *   - 4xx  -> non-retryable (the service is saying no, and will say no again)
 *   - 5xx / no response -> retryable (temporary, worth another attempt)
 */
async function post(url: string, body: unknown): Promise<void> {
  try {
    await axios.post(url, body);
  } catch (err: any) {
    const status: number | undefined = err?.response?.status;
    const code: string | undefined = err?.response?.data?.code;
    const message: string = code ?? err?.message ?? "request failed";

    if (status !== undefined && status >= 400 && status < 500) {
      throw ApplicationFailure.nonRetryable(message, code ?? "bad_request");
    }

    throw ApplicationFailure.retryable(message, code ?? "temporary_error");
  }
}

export async function authorizePayment(orderId: string, amount: string): Promise<void> {
  await post(`${PAYMENT_URL}/payment/authorize`, { orderId, amount });
}

export async function reserveInventory(orderId: string, items: CanonicalItem[]): Promise<void> {
  await post(`${INVENTORY_URL}/inventory/reserve`, { orderId, items });
}

export async function createShipment(orderId: string): Promise<void> {
  await post(`${SHIPPING_URL}/shipping/create`, { orderId });
}

export async function sendNotification(orderId: string, recipient: string): Promise<void> {
  await post(`${NOTIFICATION_URL}/notification/send`, { orderId, recipient });
}

export async function refundPayment(orderId: string): Promise<void> {
  await post(`${PAYMENT_URL}/payment/refund`, { orderId });
}

export async function releaseInventory(orderId: string): Promise<void> {
  await post(`${INVENTORY_URL}/inventory/release`, { orderId });
}