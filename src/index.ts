import { DurableObject } from "cloudflare:workers";

interface Env {
  COORDINATOR: DurableObjectNamespace<CoordinatorDO>;
  WORKER: DurableObjectNamespace<WorkerDO>;
  TEST_KV: KVNamespace;
}

interface CallResult {
  workerId: number;
  startTime: number;
  workerStartTime: number;
  endTime: number;
  duration: number;
  queueDelay: number;
}

interface TestResult {
  totalCalls: number;
  delayMs: number;
  totalDuration: number;
  results: CallResult[];
  queueAnalysis: {
    minQueueDelay: number;
    maxQueueDelay: number;
    avgQueueDelay: number;
  };
}

interface BulkWriteResult {
  processedAt: number;
  totalDuration: number;
  writes: { key: string; startTime: number; endTime: number; duration: number }[];
}

// Shared bulk KV write function - used by both Worker and DO
async function bulkWriteKv(kv: KVNamespace, numWrites: number, ttlSeconds: number): Promise<BulkWriteResult> {
  const overallStart = Date.now();
  const promises = Array.from({ length: numWrites }, async () => {
    const writeStart = Date.now();
    const key = crypto.randomUUID();
    await kv.put(key, String(writeStart), { expirationTtl: ttlSeconds });
    const writeEnd = Date.now();
    return {
      key,
      startTime: writeStart - overallStart,
      endTime: writeEnd - overallStart,
      duration: writeEnd - writeStart,
    };
  });
  const writes = await Promise.all(promises);
  return {
    processedAt: overallStart,
    totalDuration: Date.now() - overallStart,
    writes,
  };
}

function formatBulkResult(calls: number, ttl: number, result: BulkWriteResult) {
  const durations = result.writes.map((w) => w.duration);
  const endTimes = result.writes.map((w) => w.endTime);
  return {
    numWrites: calls,
    ttl,
    totalDuration: result.totalDuration,
    writes: result.writes,
    analysis: {
      minDuration: Math.min(...durations),
      maxDuration: Math.max(...durations),
      avgDuration: durations.reduce((a, b) => a + b, 0) / durations.length,
      durationSpread: Math.max(...durations) - Math.min(...durations),
      minEndTime: Math.min(...endTimes),
      maxEndTime: Math.max(...endTimes),
      endTimeSpread: Math.max(...endTimes) - Math.min(...endTimes),
    },
  };
}

// Shared test runner - takes a function that makes the actual call
async function runConcurrentTest(
  numCalls: number,
  delayMs: number,
  sameTarget: boolean,
  callFn: (workerId: number, delayMs: number) => Promise<{ processedAt: number }>
): Promise<TestResult> {
  const overallStart = Date.now();
  const promises: Promise<CallResult>[] = [];

  for (let i = 0; i < numCalls; i++) {
    const workerId = sameTarget ? 0 : i;
    const callStart = Date.now();

    const promise = (async () => {
      const { processedAt } = await callFn(workerId, delayMs);
      const callEnd = Date.now();
      const workerStartTime = processedAt - overallStart;
      const startTime = callStart - overallStart;
      return {
        workerId,
        startTime,
        workerStartTime,
        endTime: callEnd - overallStart,
        duration: callEnd - callStart,
        queueDelay: workerStartTime - startTime,
      };
    })();

    promises.push(promise);
  }

  const results = await Promise.all(promises);
  const overallEnd = Date.now();

  const queueDelays = results.map((r) => r.queueDelay);
  return {
    totalCalls: numCalls,
    delayMs,
    totalDuration: overallEnd - overallStart,
    results,
    queueAnalysis: {
      minQueueDelay: Math.min(...queueDelays),
      maxQueueDelay: Math.max(...queueDelays),
      avgQueueDelay: queueDelays.reduce((a, b) => a + b, 0) / queueDelays.length,
    },
  };
}

/**
 * WorkerDO - Simple DO that simulates work with configurable delay
 *
 * Storage wrapper: when useStorage=true, wraps each request with:
 *   1. storage.get("counter") at start (input gate blocks new events during this)
 *   2. storage.put("counter", counter+1) at end (output gate delays response until confirmed)
 * This tests if input gates serialize concurrent requests to the same DO.
 */
export class WorkerDO extends DurableObject<Env> {
  private async withStorage<T>(useStorage: boolean, fn: () => Promise<T>): Promise<T> {
    if (!useStorage) {
      return fn();
    }
    // Read at start - input gate will block new events during storage operation
    const counter = (await this.ctx.storage.get<number>("counter")) ?? 0;
    const result = await fn();
    // Write at end - output gate delays response until write confirmed
    await this.ctx.storage.put("counter", counter + 1);
    return result;
  }

  async doWork(
    workerId: number,
    delayMs: number,
    useStorage = false
  ): Promise<{ workerId: number; processedAt: number }> {
    return this.withStorage(useStorage, async () => {
      const start = Date.now();
      await new Promise((resolve) => setTimeout(resolve, delayMs));
      return { workerId, processedAt: start };
    });
  }

  async writeKv(
    workerId: number,
    ttlSeconds: number,
    useStorage = false
  ): Promise<{ workerId: number; processedAt: number; key: string }> {
    return this.withStorage(useStorage, async () => {
      const start = Date.now();
      const key = crypto.randomUUID();
      await this.env.TEST_KV.put(key, String(start), { expirationTtl: ttlSeconds });
      return { workerId, processedAt: start, key };
    });
  }

  async bulkWriteKv(numWrites: number, ttlSeconds: number, useStorage = false): Promise<BulkWriteResult> {
    return this.withStorage(useStorage, async () => {
      return bulkWriteKv(this.env.TEST_KV, numWrites, ttlSeconds);
    });
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const workerId = parseInt(url.searchParams.get("workerId") || "0");
    const action = url.searchParams.get("action") || "work";
    const useStorage = url.searchParams.get("storage") === "true";

    if (action === "kv") {
      const ttl = parseInt(url.searchParams.get("ttl") || "60");
      const result = await this.withStorage(useStorage, async () => {
        const start = Date.now();
        const key = crypto.randomUUID();
        await this.env.TEST_KV.put(key, String(start), { expirationTtl: ttl });
        return { workerId, processedAt: start, key };
      });
      return Response.json(result);
    }

    const delayMs = parseInt(url.searchParams.get("delay") || "100");
    const result = await this.withStorage(useStorage, async () => {
      const start = Date.now();
      await new Promise((resolve) => setTimeout(resolve, delayMs));
      return { workerId, processedAt: start };
    });
    return Response.json(result);
  }
}

/**
 * CoordinatorDO - Makes concurrent calls to WorkerDOs
 */
export class CoordinatorDO extends DurableObject<Env> {
  async testRpc(numCalls: number, delayMs: number, sameWorker: boolean, useStorage = false): Promise<TestResult> {
    return runConcurrentTest(numCalls, delayMs, sameWorker, async (workerId, delay) => {
      const stub = this.env.WORKER.get(this.env.WORKER.idFromName(`worker-${workerId}`));
      return stub.doWork(workerId, delay, useStorage);
    });
  }

  async testFetch(numCalls: number, delayMs: number, sameWorker: boolean, useStorage = false): Promise<TestResult> {
    return runConcurrentTest(numCalls, delayMs, sameWorker, async (workerId, delay) => {
      const stub = this.env.WORKER.get(this.env.WORKER.idFromName(`worker-${workerId}`));
      const res = await stub.fetch(`http://worker?workerId=${workerId}&delay=${delay}&storage=${useStorage}`);
      return res.json() as Promise<{ processedAt: number }>;
    });
  }
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);

    if (url.pathname === "/" || url.pathname === "/help") {
      return new Response(
        `Concurrency Test Worker

All DO endpoints support &storage=true to wrap with storage.get/put (tests input gates)

DO-to-DO:
  /test/do/rpc?calls=N&delay=MS&same=BOOL&storage=BOOL
  /test/do/fetch?calls=N&delay=MS&same=BOOL&storage=BOOL

Worker-to-DO:
  /test/worker/rpc?calls=N&delay=MS&same=BOOL&storage=BOOL
  /test/worker/fetch?calls=N&delay=MS&same=BOOL&storage=BOOL

KV Write (Worker -> single DO -> N random KV keys):
  /test/kv/rpc?calls=N&ttl=SECONDS&storage=BOOL
  /test/kv/fetch?calls=N&ttl=SECONDS&storage=BOOL

KV Bulk (single request -> N concurrent KV writes):
  /test/kv/bulk?calls=N&ttl=SECONDS&storage=BOOL   (from DO)
  /test/kv/bulk-worker?calls=N&ttl=SECONDS         (from Worker, no storage option)
`,
        { headers: { "Content-Type": "text/plain" } }
      );
    }

    const calls = parseInt(url.searchParams.get("calls") || "10");
    const delay = parseInt(url.searchParams.get("delay") || "100");
    const same = url.searchParams.get("same") === "true";
    const storage = url.searchParams.get("storage") === "true";

    // DO-to-DO tests
    if (url.pathname === "/test/do/rpc" || url.pathname === "/test/do/fetch") {
      const coordinator = env.COORDINATOR.get(env.COORDINATOR.idFromName("coordinator"));
      const result =
        url.pathname === "/test/do/rpc"
          ? await coordinator.testRpc(calls, delay, same, storage)
          : await coordinator.testFetch(calls, delay, same, storage);
      return Response.json(result);
    }

    // Worker-to-DO tests
    if (url.pathname === "/test/worker/rpc") {
      const result = await runConcurrentTest(calls, delay, same, async (workerId, delayMs) => {
        const stub = env.WORKER.get(env.WORKER.idFromName(`worker-${workerId}`));
        return stub.doWork(workerId, delayMs, storage);
      });
      return Response.json(result);
    }

    if (url.pathname === "/test/worker/fetch") {
      const result = await runConcurrentTest(calls, delay, same, async (workerId, delayMs) => {
        const stub = env.WORKER.get(env.WORKER.idFromName(`worker-${workerId}`));
        const res = await stub.fetch(`http://worker?workerId=${workerId}&delay=${delayMs}&storage=${storage}`);
        return res.json() as Promise<{ processedAt: number }>;
      });
      return Response.json(result);
    }

    // KV write tests - Worker sends N requests to single DO, each writes to random KV key
    const ttl = parseInt(url.searchParams.get("ttl") || "60");

    if (url.pathname === "/test/kv/rpc") {
      const stub = env.WORKER.get(env.WORKER.idFromName("worker-0"));
      const result = await runConcurrentTest(calls, delay, same, async (workerId) => {
        return stub.writeKv(workerId, ttl, storage);
      });
      return Response.json(result);
    }

    if (url.pathname === "/test/kv/fetch") {
      const stub = env.WORKER.get(env.WORKER.idFromName("worker-0"));
      const result = await runConcurrentTest(calls, delay, same, async (workerId) => {
        const res = await stub.fetch(`http://worker?workerId=${workerId}&action=kv&ttl=${ttl}&storage=${storage}`);
        return res.json() as Promise<{ processedAt: number }>;
      });
      return Response.json(result);
    }

    // Single request to DO that does N concurrent KV writes internally
    if (url.pathname === "/test/kv/bulk") {
      const stub = env.WORKER.get(env.WORKER.idFromName("worker-0"));
      const result = await stub.bulkWriteKv(calls, ttl, storage);
      return Response.json(formatBulkResult(calls, ttl, result));
    }

    // Single request to Worker that does N concurrent KV writes internally
    if (url.pathname === "/test/kv/bulk-worker") {
      const result = await bulkWriteKv(env.TEST_KV, calls, ttl);
      return Response.json(formatBulkResult(calls, ttl, result));
    }

    return new Response("Not Found", { status: 404 });
  },
};
