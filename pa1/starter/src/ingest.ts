/**
 * PA1 — legacy file ingestion.
 *
 * You are reading two files that a system you do not control exports for you:
 *
 *   data/orders-20260901.txt   fixed-width, CP1257 ("windows-1257")
 *   data/customers.csv         semicolon-separated, UTF-8
 *
 * Yes, two different encodings in one integration. That is not a trick I
 * invented; it is Tuesday.
 *
 * Everything you need is in the Node standard library. Do not add a parsing,
 * CSV or encoding dependency — feeling where these files fight back is the
 * entire point of the assignment, and a public test checks for it.
 */

import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

// ---------------------------------------------------------------- contract --
// The grader calls ingest() directly, with its own input files. Do not change
// its name, its parameters or the shape of what it returns. Everything else in
// this file is yours to restructure.

export interface Order {
  orderId: string;
  customerId: string;
  /** Correctly decoded and trimmed. "Bērziņš", never "B?rzi??". */
  customerName: string;
  /** ISO-8601 calendar date: "2026-09-01". */
  orderDate: string;
  /** Decimal STRING, never a number: "1234.56", "-250.00", "0.00". */
  amount: string;
  currency: string;
}

export interface RejectedRecord {
  /** 1-based line number in the orders file. */
  line: number;
  /** The offending line, as you decoded it. */
  raw: string;
  /** Why you rejected it, in plain language. */
  reason: string;
}

export interface Report {
  orders: Order[];
  rejected: RejectedRecord[];
  /** Customer ids that appear in the CSV but on no accepted order. */
  unmatchedCustomers: string[];
}

export interface IngestOptions {
  ordersPath: string;
  customersPath: string;
}

// ------------------------------------------------------------------ layout --

export const ORDER_LAYOUT = {
  orderId: [0, 10],
  customerId: [10, 20],
  customerName: [20, 52],
  orderDate: [52, 62],
  amount: [62, 74],
  currency: [74, 77],
} as const;

export const ORDER_LINE_LENGTH = 77;

// ------------------------------------------------------------------- paths --

const PA1_ROOT = fileURLToPath(new URL("../../", import.meta.url));

export const DEFAULT_ORDERS_PATH = path.join(PA1_ROOT, "data", "orders-20260901.txt");
export const DEFAULT_CUSTOMERS_PATH = path.join(PA1_ROOT, "data", "customers.csv");
export const OUTPUT_PATH = path.join(PA1_ROOT, "out", "report.json");

// ------------------------------------------------------------------- steps --

export function decodeOrderFile(bytes: Buffer): string {
  const decoder = new TextDecoder("windows-1257");
  return decoder.decode(bytes);
}

export function toIsoDate(ddmmyyyy: string): string {
  const [day, month, year] = ddmmyyyy.split(".");
  return `${year}-${month}-${day}`;
}

export function toDecimalString(amount: string): string {
  return amount.trim().replace(",", ".");
}

export function parseCustomers(csv: string): Map<string, string> {
  const lines = csv.split(/\r\n|\r|\n/).filter((line) => line.trim() !== "");
  const map = new Map<string, string>();

  for (let i = 1; i < lines.length; i++) {
    const line = lines[i];
    if (!line) continue;

    const parts = line.split(";");
    const customerId = parts[0];
    const fullName = parts[1];
    if (!customerId || !fullName) continue;

    map.set(customerId.trim(), fullName.trim());
  }

  return map;
}

// ------------------------------------------------------------------ ingest --

export function ingest(options: IngestOptions): Report {
  const rawBytes = readFileSync(options.ordersPath);
  const text = decodeOrderFile(rawBytes);

  const lines = text.split(/\r\n|\r|\n/).filter((line) => line.length > 0);

  const orders: Order[] = [];
  const rejected: RejectedRecord[] = [];
  const referencedCustomerIds = new Set<string>();

  lines.forEach((line, index) => {
    const lineNumber = index + 1;

    if (line.length !== ORDER_LINE_LENGTH) {
      rejected.push({
        line: lineNumber,
        raw: line,
        reason: `expected ${ORDER_LINE_LENGTH} characters, got ${line.length}`,
      });
      return;
    }

    const orderId = line.slice(...ORDER_LAYOUT.orderId).trim();
    const customerId = line.slice(...ORDER_LAYOUT.customerId).trim();
    const customerName = line.slice(...ORDER_LAYOUT.customerName).trim();
    const orderDateRaw = line.slice(...ORDER_LAYOUT.orderDate).trim();
    const amountRaw = line.slice(...ORDER_LAYOUT.amount).trim();
    const currency = line.slice(...ORDER_LAYOUT.currency).trim();

    orders.push({
      orderId,
      customerId,
      customerName,
      orderDate: toIsoDate(orderDateRaw),
      amount: toDecimalString(amountRaw),
      currency,
    });

    referencedCustomerIds.add(customerId);
  });

  const customersCsv = readFileSync(options.customersPath, "utf8");
  const customers = parseCustomers(customersCsv);

  const unmatchedCustomers: string[] = [];
  for (const customerId of customers.keys()) {
    if (!referencedCustomerIds.has(customerId)) {
      unmatchedCustomers.push(customerId);
    }
  }

  return { orders, rejected, unmatchedCustomers };
}

// -------------------------------------------------------------------- main --

/** Writes the report to pa1/out/report.json. Run with: npm start */
export function main(): void {
  const report = ingest({
    ordersPath: DEFAULT_ORDERS_PATH,
    customersPath: DEFAULT_CUSTOMERS_PATH,
  });

  mkdirSync(path.dirname(OUTPUT_PATH), { recursive: true });
  writeFileSync(OUTPUT_PATH, JSON.stringify(report, null, 2) + "\n", "utf8");

  console.log(
    `wrote ${OUTPUT_PATH}\n` +
      `  ${report.orders.length} orders\n` +
      `  ${report.rejected.length} rejected\n` +
      `  ${report.unmatchedCustomers.length} customers with no order`,
  );
}

// Only run main() when this file is executed directly, not when it is imported
// by the tests.
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main();
}