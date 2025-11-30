import { DurableObject } from "cloudflare:workers";

interface Env {
  COORDINATOR: DurableObjectNamespace<CoordinatorDO>;
  WORKER: DurableObjectNamespace<WorkerDO>;
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
 */
export class WorkerDO extends DurableObject {
  async doWork(workerId: number, delayMs: number): Promise<{ workerId: number; processedAt: number }> {
    const start = Date.now();
    await new Promise((resolve) => setTimeout(resolve, delayMs));
    return { workerId, processedAt: start };
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const workerId = parseInt(url.searchParams.get("workerId") || "0");
    const delayMs = parseInt(url.searchParams.get("delay") || "100");

    const start = Date.now();
    await new Promise((resolve) => setTimeout(resolve, delayMs));
    return Response.json({ workerId, processedAt: start });
  }
}

/**
 * CoordinatorDO - Makes concurrent calls to WorkerDOs
 */
export class CoordinatorDO extends DurableObject<Env> {
  async testRpc(numCalls: number, delayMs: number, sameWorker: boolean): Promise<TestResult> {
    return runConcurrentTest(numCalls, delayMs, sameWorker, async (workerId, delay) => {
      const stub = this.env.WORKER.get(this.env.WORKER.idFromName(`worker-${workerId}`));
      return stub.doWork(workerId, delay);
    });
  }

  async testFetch(numCalls: number, delayMs: number, sameWorker: boolean): Promise<TestResult> {
    return runConcurrentTest(numCalls, delayMs, sameWorker, async (workerId, delay) => {
      const stub = this.env.WORKER.get(this.env.WORKER.idFromName(`worker-${workerId}`));
      const res = await stub.fetch(`http://worker?workerId=${workerId}&delay=${delay}`);
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

DO-to-DO:
  /test/do/rpc?calls=N&delay=MS&same=BOOL
  /test/do/fetch?calls=N&delay=MS&same=BOOL

Worker-to-DO:
  /test/worker/rpc?calls=N&delay=MS&same=BOOL
  /test/worker/fetch?calls=N&delay=MS&same=BOOL
`,
        { headers: { "Content-Type": "text/plain" } }
      );
    }

    const calls = parseInt(url.searchParams.get("calls") || "10");
    const delay = parseInt(url.searchParams.get("delay") || "100");
    const same = url.searchParams.get("same") === "true";

    // DO-to-DO tests
    if (url.pathname === "/test/do/rpc" || url.pathname === "/test/do/fetch") {
      const coordinator = env.COORDINATOR.get(env.COORDINATOR.idFromName("coordinator"));
      const result =
        url.pathname === "/test/do/rpc"
          ? await coordinator.testRpc(calls, delay, same)
          : await coordinator.testFetch(calls, delay, same);
      return Response.json(result);
    }

    // Worker-to-DO tests
    if (url.pathname === "/test/worker/rpc") {
      const result = await runConcurrentTest(calls, delay, same, async (workerId, delayMs) => {
        const stub = env.WORKER.get(env.WORKER.idFromName(`worker-${workerId}`));
        return stub.doWork(workerId, delayMs);
      });
      return Response.json(result);
    }

    if (url.pathname === "/test/worker/fetch") {
      const result = await runConcurrentTest(calls, delay, same, async (workerId, delayMs) => {
        const stub = env.WORKER.get(env.WORKER.idFromName(`worker-${workerId}`));
        const res = await stub.fetch(`http://worker?workerId=${workerId}&delay=${delayMs}`);
        return res.json() as Promise<{ processedAt: number }>;
      });
      return Response.json(result);
    }

    return new Response("Not Found", { status: 404 });
  },
};
