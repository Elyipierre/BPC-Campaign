import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";

const THIS_DIR = path.dirname(fileURLToPath(import.meta.url));
const SERVER_PATH = path.resolve(THIS_DIR, "../server/overture-api.cjs");

describe("alignment preview/apply api wiring", () => {
  it("exposes preview/apply local-data endpoints", () => {
    const source = fs.readFileSync(SERVER_PATH, "utf8");
    expect(source.includes("/api/local-data/territories/align/preview")).toBe(true);
    expect(source.includes("/api/local-data/territories/align/apply")).toBe(true);
    expect(source.includes("confirmToken")).toBe(true);
  });
});
