/**
 * PA6 orchestrator — full checkout saga implementation.
 */
import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import express, { type Request, type Response } from "express";
import axios from "axios";
import Ajv from "ajv";
import addFormats from "ajv-formats";
import type { CanonicalOrder } from "./canonical-order";

const app = express();
app.use(express.json());
app.use(express.static(path.join(process.cwd(), "public")));

interface Config {
  port: number;
  paymentUrl: string;
  inventoryUrl: string;
  shippingUrl: string;
  notificationUrl: string;
  requestTimeoutMs: number;
}

function readRequiredEnv(name: string): string {
  const value = process.env[name];
  if (!value) {
    throw new Error(`Missing required environment variable: ${name}`);
  }
  return value;
}

function loadConfig(): Config {
  return {
    port: Number(process.env.ORCHESTRATOR_PORT || 3000),
    paymentUrl: readRequiredEnv("PAYMENT_URL"),
    inventoryUrl: readRequiredEnv("INVENTORY_URL"),
    shippingUrl: readRequiredEnv("SHIPPING_URL"),
    notificationUrl: readRequiredEnv("NOTIFICATION_URL"),
    requestTimeoutMs: Number(process.env.REQUEST_TIMEOUT_MS || 2500),
  };
}

const config = loadConfig();

const DATA_DIR = "/data";
const IDEMPOTENCY_STORE_PATH = path.join(DATA_DIR, "idempotency-store.json");
const SAGA_STORE_PATH = path.join(DATA_DIR, "saga-store.json");

export interface TraceItem {
  step: string;
  status: string;
  startedAt: string;
  finishedAt: string;
  durationMs: number;
}

export type SagaState = "completed" | "failed" | "compensated";
export type IdempotencyState = "in_progress" | "completed" | "failed" | "compensated";

interface IdempotencyRecord {
  requestHash: string;
  state: IdempotencyState;
  httpStatus: number;
  response: unknown;
  updatedAt: string;
}

interface IdempotencyStore {
  records: Record<string, IdempotencyRecord>;
}

interface SagaRecord {
  idempotencyKey: string;
  state: SagaState;
  steps: TraceItem[];
  updatedAt: string;
}

interface SagaStore {
  sagas: Record<string, SagaRecord>;
}

function ensureJsonFile(filePath: string, initialData: unknown): void {
  const dirPath = path.dirname(filePath);
  if (!fs.existsSync(dirPath)) {
    fs.mkdirSync(dirPath, { recursive: true });
  }
  if (!fs.existsSync(filePath)) {
    fs.writeFileSync(filePath, JSON.stringify(initialData, null, 2), "utf8");
  }
}

function readJsonFile<T>(filePath: string): T {
  ensureJsonFile(filePath, {});
  const raw = fs.readFileSync(filePath, "utf8");
  return JSON.parse(raw || "{}") as T;
}

function writeJsonFile(filePath: string, value: unknown): void {
  fs.writeFileSync(filePath, JSON.stringify(value, null, 2), "utf8");
}

function nowIso(): string {
  return new Date().toISOString();
}

function payloadHash(payload: unknown): string {
  const normalized = JSON.stringify(payload);
  const hash = crypto.createHash("sha256").update(normalized).digest("hex");
  return `sha256:${hash}`;
}

// ------------------------------------------------------------- money math --
// unitPrice is a decimal string ("22.50"), never a JSON number. Convert to
// whole cents via string splitting (never parseFloat * 100 — that trap is
// documented in README.md) so 18.90 stays exactly 1890 cents, not
// 1889.9999999999998.

function toCents(decimalString: string): number {
  const negative = decimalString.startsWith("-");
  const unsigned = negative ? decimalString.slice(1) : decimalString;
  const [wholePart, fractionRaw = ""] = unsigned.split(".");
  const fractionPart = fractionRaw.padEnd(2, "0").slice(0, 2);
  const cents = parseInt(wholePart, 10) * 100 + parseInt(fractionPart, 10);
  return negative ? -cents : cents;
}

function centsToDecimalString(cents: number): string {
  const negative = cents < 0;
  const abs = Math.abs(cents);
  const wholePart = Math.floor(abs / 100);
  const fractionPart = String(abs % 100).padStart(2, "0");
  return `${negative ? "-" : ""}${wholePart}.${fractionPart}`;
}

function computeAmount(order: CanonicalOrder): string {
  let totalCents = 0;
  for (const item of order.items) {
    totalCents += toCents(item.unitPrice) * item.quantity;
  }
  return centsToDecimalString(totalCents);
}

// The request body is a canonical order. docker-compose.yml mounts the
// course's canonical/ folder read-only at /canonical.
const CANONICAL_SCHEMA_PATH = "/canonical/order.schema.json";

if (!fs.existsSync(CANONICAL_SCHEMA_PATH)) {
  throw new Error(
    `${CANONICAL_SCHEMA_PATH} not found. docker-compose.yml mounts ../canonical ` +
      "from your repository root: copy canonical/ from eai-2026 next to pa6/, " +
      "then run docker compose up again.",
  );
}

const ajv = new Ajv({ allErrors: true });
addFormats(ajv);
const validateCanonicalOrder = ajv.compile(JSON.parse(fs.readFileSync(CANONICAL_SCHEMA_PATH, "utf8")));

function validateCheckoutPayload(payload: unknown): string | null {
  if (validateCanonicalOrder(payload)) {
    return null;
  }
  return `Request body is not a canonical order: ${ajv.errorsText(validateCanonicalOrder.errors, { dataVar: "body" })}`;
}

function bootstrapStores(): void {
  ensureJsonFile(IDEMPOTENCY_STORE_PATH, { records: {} });
  ensureJsonFile(SAGA_STORE_PATH, { sagas: {} });
}

// -------------------------------------------------------------- one call --
// Runs one downstream POST, timing it and classifying the outcome as
// success / failed / timeout. Never throws — the saga logic below only
// ever inspects the returned result.

interface StepResult {
  status: "success" | "failed" | "timeout";
  code?: string;
  data?: unknown;
  trace: TraceItem;
}

async function callStep(stepName: string, url: string, body: unknown, timeoutMs: number): Promise<StepResult> {
  const startedAt = nowIso();
  const start = Date.now();

  try {
    const response = await axios.post(url, body, { timeout: timeoutMs });
    return {
      status: "success",
      data: response.data,
      trace: {
        step: stepName,
        status: "success",
        startedAt,
        finishedAt: nowIso(),
        durationMs: Date.now() - start,
      },
    };
  } catch (err: any) {
    const finishedAt = nowIso();
    const durationMs = Date.now() - start;
    const isTimeout = err?.code === "ECONNABORTED" || /timeout/i.test(String(err?.message ?? ""));

    if (isTimeout) {
      return {
        status: "timeout",
        code: "timeout",
        trace: { step: stepName, status: "timeout", startedAt, finishedAt, durationMs },
      };
    }

    const code = err?.response?.data?.code ?? "downstream_error";
    return {
      status: "failed",
      code,
      data: err?.response?.data,
      trace: { step: stepName, status: "failed", startedAt, finishedAt, durationMs },
    };
  }
}

// ------------------------------------------------------------ the saga --

interface SagaOutcome {
  httpStatus: number;
  response: {
    orderId: string;
    status: "completed" | "failed" | "compensated";
    code?: string;
    trace: TraceItem[];
  };
  sagaState: SagaState;
}

/**
 * Undoes every completed step that has a compensating action, in reverse
 * order (most recently completed first). Only "payment" and "inventory" are
 * undoable — shipping and notification have no compensating endpoint, so a
 * failure at either of those still only rolls back inventory then payment.
 */
async function compensate(
  orderId: string,
  completedUndoableSteps: string[],
  trace: TraceItem[],
): Promise<{ ok: boolean }> {
  let ok = true;

  for (const step of [...completedUndoableSteps].reverse()) {
    if (step === "inventory") {
      const result = await callStep(
        "inventory_release",
        `${config.inventoryUrl}/inventory/release`,
        { orderId },
        config.requestTimeoutMs,
      );
      trace.push(result.trace);
      if (result.status !== "success") ok = false;
    } else if (step === "payment") {
      const result = await callStep(
        "payment_refund",
        `${config.paymentUrl}/payment/refund`,
        { orderId },
        config.requestTimeoutMs,
      );
      trace.push(result.trace);
      if (result.status !== "success") ok = false;
    }
  }

  return { ok };
}

async function runSaga(order: CanonicalOrder, amount: string, recipient: string): Promise<SagaOutcome> {
  const orderId = order.orderId;
  const trace: TraceItem[] = [];
  const completedUndoableSteps: string[] = [];

  // 1) payment — nothing has run yet, so a failure here needs no compensation.
  const paymentResult = await callStep(
    "payment",
    `${config.paymentUrl}/payment/authorize`,
    { orderId, amount },
    config.requestTimeoutMs,
  );
  trace.push(paymentResult.trace);

  if (paymentResult.status !== "success") {
    return {
      httpStatus: paymentResult.status === "timeout" ? 504 : 422,
      response: { orderId, status: "failed", code: paymentResult.code, trace },
      sagaState: "failed",
    };
  }
  completedUndoableSteps.push("payment");

  // 2) inventory
  const inventoryResult = await callStep(
    "inventory",
    `${config.inventoryUrl}/inventory/reserve`,
    { orderId, items: order.items },
    config.requestTimeoutMs,
  );
  trace.push(inventoryResult.trace);

  if (inventoryResult.status !== "success") {
    const { ok } = await compensate(orderId, completedUndoableSteps, trace);
    if (!ok) {
      return {
        httpStatus: 422,
        response: { orderId, status: "failed", code: "compensation_failed", trace },
        sagaState: "failed",
      };
    }
    return {
      httpStatus: inventoryResult.status === "timeout" ? 504 : 422,
      response: { orderId, status: "compensated", code: inventoryResult.code, trace },
      sagaState: "compensated",
    };
  }
  completedUndoableSteps.push("inventory");

  // 3) shipping — no compensating endpoint, so a failure here still only
  // rolls back inventory then payment.
  const shippingResult = await callStep(
    "shipping",
    `${config.shippingUrl}/shipping/create`,
    { orderId },
    config.requestTimeoutMs,
  );
  trace.push(shippingResult.trace);

  if (shippingResult.status !== "success") {
    const { ok } = await compensate(orderId, completedUndoableSteps, trace);
    if (!ok) {
      return {
        httpStatus: 422,
        response: { orderId, status: "failed", code: "compensation_failed", trace },
        sagaState: "failed",
      };
    }
    return {
      httpStatus: shippingResult.status === "timeout" ? 504 : 422,
      response: { orderId, status: "compensated", code: shippingResult.code, trace },
      sagaState: "compensated",
    };
  }

  // 4) notification — also no compensating endpoint.
  const notificationResult = await callStep(
    "notification",
    `${config.notificationUrl}/notification/send`,
    { orderId, recipient },
    config.requestTimeoutMs,
  );
  trace.push(notificationResult.trace);

  if (notificationResult.status !== "success") {
    const { ok } = await compensate(orderId, completedUndoableSteps, trace);
    if (!ok) {
      return {
        httpStatus: 422,
        response: { orderId, status: "failed", code: "compensation_failed", trace },
        sagaState: "failed",
      };
    }
    return {
      httpStatus: notificationResult.status === "timeout" ? 504 : 422,
      response: { orderId, status: "compensated", code: notificationResult.code, trace },
      sagaState: "compensated",
    };
  }

  // all four succeeded
  return {
    httpStatus: 200,
    response: { orderId, status: "completed", trace },
    sagaState: "completed",
  };
}

// --------------------------------------------------------------- routes --

app.get("/health", (_req: Request, res: Response) => {
  res.status(200).json({ status: "ok" });
});

app.get("/debug/trace/:orderId", (req: Request, res: Response) => {
  const orderIdParam = req.params.orderId ?? "";
  const sagaStore = readJsonFile<SagaStore>(SAGA_STORE_PATH);
  const saga = sagaStore?.sagas?.[orderIdParam];
  if (!saga) {
    res.status(404).json({ code: "not_found", message: "No saga found for this orderId" });
    return;
  }
  res.status(200).json(saga);
});

app.post("/checkout", async (req: Request, res: Response) => {
  const idempotencyKey = req.header("Idempotency-Key");
  if (!idempotencyKey) {
    res.status(400).json({
      code: "validation_error",
      message: "Idempotency-Key header is required",
    });
    return;
  }

  const validationError = validateCheckoutPayload(req.body);
  if (validationError) {
    res.status(400).json({
      code: "validation_error",
      message: validationError,
    });
    return;
  }

  const requestHash = payloadHash(req.body);
  const idempotencyStore = readJsonFile<IdempotencyStore>(IDEMPOTENCY_STORE_PATH);
  if (!idempotencyStore.records) {
    idempotencyStore.records = {};
  }

  const existing = idempotencyStore.records[idempotencyKey];
  if (existing) {
    if (existing.requestHash !== requestHash) {
      res.status(409).json({
        code: "idempotency_payload_mismatch",
        message: "This Idempotency-Key is already used for a different payload",
      });
      return;
    }

    if (existing.state === "in_progress") {
      res.status(409).json({
        code: "idempotency_conflict",
        message: "A request with this Idempotency-Key is still running",
      });
      return;
    }

    // Same key, same payload, and the earlier attempt already finished —
    // replay its exact result. The saga does not run again.
    res.status(existing.httpStatus).json(existing.response);
    return;
  }

  const order = req.body as CanonicalOrder;
  const orderId = order.orderId;

  // Mark this key in_progress BEFORE any downstream call, and write it to
  // disk synchronously — so a second request arriving during the saga sees
  // in_progress and gets idempotency_conflict, rather than racing ahead.
  idempotencyStore.records[idempotencyKey] = {
    requestHash,
    state: "in_progress",
    httpStatus: 202,
    response: { orderId, status: "in_progress", trace: [] },
    updatedAt: nowIso(),
  };
  writeJsonFile(IDEMPOTENCY_STORE_PATH, idempotencyStore);

  const amount = computeAmount(order);
  const recipient = order.customer.email;

  const { httpStatus, response, sagaState } = await runSaga(order, amount, recipient);

  const sagaStore = readJsonFile<SagaStore>(SAGA_STORE_PATH);
  if (!sagaStore.sagas) {
    sagaStore.sagas = {};
  }
  sagaStore.sagas[orderId] = {
    idempotencyKey,
    state: sagaState,
    steps: response.trace,
    updatedAt: nowIso(),
  };
  writeJsonFile(SAGA_STORE_PATH, sagaStore);

  const finalIdempotencyStore = readJsonFile<IdempotencyStore>(IDEMPOTENCY_STORE_PATH);
  if (!finalIdempotencyStore.records) {
    finalIdempotencyStore.records = {};
  }
  finalIdempotencyStore.records[idempotencyKey] = {
    requestHash,
    state: sagaState,
    httpStatus,
    response,
    updatedAt: nowIso(),
  };
  writeJsonFile(IDEMPOTENCY_STORE_PATH, finalIdempotencyStore);

  res.status(httpStatus).json(response);
});

bootstrapStores();

app.listen(config.port, () => {
  console.log(`[orchestrator] listening on port ${config.port}`);
  console.log("[orchestrator] downstream targets loaded from env", {
    paymentUrl: config.paymentUrl,
    inventoryUrl: config.inventoryUrl,
    shippingUrl: config.shippingUrl,
    notificationUrl: config.notificationUrl,
    requestTimeoutMs: config.requestTimeoutMs,
  });
});