import { describe, it, expect } from "bun:test";
import { createDeferredPromise } from "../../packages/kstate/utils/deferred-promise";

describe("createDeferredPromise", () => {
  it("should resolve a promise", async () => {
    const deferred = createDeferredPromise();
    setTimeout(() => deferred.resolve(42), 10);
    const result = await deferred.promise;
    expect(result).toBe(42);
  });
});
