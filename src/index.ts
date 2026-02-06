import { DurableObject } from "cloudflare:workers";

interface Env {
  COORDINATOR: DurableObjectNamespace<CoordinatorDO>;
  WORKER: DurableObjectNamespace<WorkerDO>;
  QUEUE_COORDINATOR: DurableObjectNamespace<QueueCoordinatorDO>;
  DISPATCHER: DurableObjectNamespace<DispatcherDO>;
  TEST_KV: KVNamespace;
  TEST_QUEUE: Queue<QueueMessage>;
}

interface QueueMessage {
  testId: string;
  messageId: number;
  enqueuedAt: number;
}

interface QueueMessageResult {
  messageId: number;
  enqueuedAt: number;
  processedAt: number;
  completedAt: number;
  workDuration: number;
  batchId: string;
}

interface QueueEnqueueResult {
  messageId: number;
  startTime: number;
  endTime: number;
  duration: number;
}

interface QueueTestResult {
  testId: string;
  totalMessages: number;
  enqueue: {
    totalDuration: number;
    results: QueueEnqueueResult[];
    analysis: {
      minDuration: number;
      maxDuration: number;
      avgDuration: number;
    };
  };
  e2e?: {
    results: QueueMessageResult[];
    analysis: {
      minLatency: number;
      maxLatency: number;
      avgLatency: number;
      minWorkDuration: number;
      maxWorkDuration: number;
      avgWorkDuration: number;
    };
    batches: {
      batchId: string;
      size: number;
      messageIds: number[];
    }[];
  };
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
  callFn: (workerId: number, delayMs: number, index: number) => Promise<{ processedAt: number }>
): Promise<TestResult> {
  const overallStart = Date.now();
  const promises: Promise<CallResult>[] = [];

  for (let i = 0; i < numCalls; i++) {
    const workerId = sameTarget ? 0 : i;
    const callStart = Date.now();

    const promise = (async () => {
      const { processedAt } = await callFn(workerId, delayMs, i);
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

  async doRandomWork(): Promise<{ processedAt: number; completedAt: number; workDuration: number }> {
    const processedAt = Date.now();
    const delayMs = 100 + Math.floor(Math.random() * 400); // 100-500ms
    await new Promise((resolve) => setTimeout(resolve, delayMs));
    const completedAt = Date.now();
    return { processedAt, completedAt, workDuration: completedAt - processedAt };
  }

  async ack(workerId: number, workMs: number = 0): Promise<{ workerId: number; processedAt: number; completedAt: number }> {
    const processedAt = Date.now();
    if (workMs > 0) {
      await new Promise((resolve) => setTimeout(resolve, workMs));
    }
    return { workerId, processedAt, completedAt: Date.now() };
  }

  async enqueueMessage(testId: string, messageId: number): Promise<{ processedAt: number }> {
    const processedAt = Date.now();
    const message: QueueMessage = {
      testId,
      messageId,
      enqueuedAt: processedAt,
    };
    await this.env.TEST_QUEUE.send(message);
    return { processedAt };
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

/**
 * QueueCoordinatorDO - Coordinates queue test results
 * Tracks messages sent and receives completion reports from queue consumer
 */
export class QueueCoordinatorDO extends DurableObject<Env> {
  async startTest(testId: string, numMessages: number): Promise<void> {
    await this.ctx.storage.put(`test:${testId}:expected`, numMessages);
    await this.ctx.storage.put(`test:${testId}:results`, []);
  }

  async recordProcessed(
    testId: string,
    messageId: number,
    enqueuedAt: number,
    processedAt: number,
    completedAt: number,
    workDuration: number,
    batchId: string
  ): Promise<void> {
    const results = (await this.ctx.storage.get<QueueMessageResult[]>(`test:${testId}:results`)) ?? [];
    results.push({ messageId, enqueuedAt, processedAt, completedAt, workDuration, batchId });
    await this.ctx.storage.put(`test:${testId}:results`, results);
  }

  async getResults(testId: string): Promise<{ expected: number; received: number; results: QueueMessageResult[] }> {
    const expected = (await this.ctx.storage.get<number>(`test:${testId}:expected`)) ?? 0;
    const results = (await this.ctx.storage.get<QueueMessageResult[]>(`test:${testId}:results`)) ?? [];
    return { expected, received: results.length, results };
  }

  async cleanup(testId: string): Promise<void> {
    await this.ctx.storage.delete(`test:${testId}:expected`);
    await this.ctx.storage.delete(`test:${testId}:results`);
  }
}

interface DispatchTask {
  id: number;
  worker_id: number;
  test_id: string;
  status: string;
  created_at: number;
  executed_at: number | null;
  completed_at: number | null;
  error: string | null;
}

interface DispatchTestResult {
  test_id: string;
  started_at: number;
  completed_at: number | null;
  total_tasks: number;
  completed_tasks: number;
  failed_tasks: number;
  alarm_count: number;
  status: string;
}

/**
 * DispatcherDO - Singleton DO that dispatches to 2000+ worker DOs using recursive alarms
 *
 * Each alarm() invocation gets a fresh 1000 sub-request quota.
 * By scheduling immediate alarms (setAlarm(Date.now())), we chain unlimited
 * total sub-requests across multiple alarm invocations.
 */
export class DispatcherDO extends DurableObject<Env> {
  private sql: SqlStorage;

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    this.sql = ctx.storage.sql;

    // Initialize schema
    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS tasks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        worker_id INTEGER NOT NULL,
        test_id TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'pending',
        created_at INTEGER NOT NULL,
        executed_at INTEGER,
        completed_at INTEGER,
        error TEXT
      );

      CREATE TABLE IF NOT EXISTS test_results (
        test_id TEXT PRIMARY KEY,
        started_at INTEGER NOT NULL,
        completed_at INTEGER,
        total_tasks INTEGER NOT NULL,
        completed_tasks INTEGER DEFAULT 0,
        failed_tasks INTEGER DEFAULT 0,
        alarm_count INTEGER DEFAULT 0,
        status TEXT DEFAULT 'running'
      );

      CREATE TABLE IF NOT EXISTS worker_timings (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        test_id TEXT NOT NULL,
        worker_id INTEGER NOT NULL,
        alarm_num INTEGER NOT NULL,
        rpc_sent_at INTEGER NOT NULL,
        processed_at INTEGER NOT NULL,
        completed_at INTEGER NOT NULL
      );

      CREATE INDEX IF NOT EXISTS idx_tasks_test_status ON tasks(test_id, status);
      CREATE INDEX IF NOT EXISTS idx_timings_test ON worker_timings(test_id);
    `);
  }

  async startTest(numTasks: number, batchSize: number, workMs: number = 0): Promise<{ testId: string; status: string; totalTasks: number }> {
    const testId = crypto.randomUUID();
    const now = Date.now();

    // Cancel any existing alarm and clear previous test
    await this.ctx.storage.deleteAlarm();
    await this.ctx.storage.delete("current_test_id");

    // Clean up old test data
    this.sql.exec(`DELETE FROM tasks`);
    this.sql.exec(`DELETE FROM test_results`);
    this.sql.exec(`DELETE FROM worker_timings`);

    // Store config for alarm to use
    await this.ctx.storage.put("batch_size", batchSize);
    await this.ctx.storage.put("work_ms", workMs);
    await this.ctx.storage.put("alarm_num", 0);

    // Insert test result record
    this.sql.exec(
      `INSERT INTO test_results (test_id, started_at, total_tasks, status) VALUES (?, ?, ?, 'running')`,
      testId,
      now,
      numTasks
    );

    // Seed tasks
    for (let i = 0; i < numTasks; i++) {
      this.sql.exec(
        `INSERT INTO tasks (worker_id, test_id, status, created_at) VALUES (?, ?, 'pending', ?)`,
        i,
        testId,
        now
      );
    }

    // Store current test ID and trigger first alarm
    await this.ctx.storage.put("current_test_id", testId);
    await this.ctx.storage.setAlarm(Date.now());

    return { testId, status: "started", totalTasks: numTasks };
  }

  async getTestStatus(testId: string): Promise<DispatchTestResult | null> {
    const result = this.sql.exec(`SELECT * FROM test_results WHERE test_id = ?`, testId).toArray();
    if (result.length === 0) return null;
    const row = result[0];
    return {
      test_id: row.test_id as string,
      started_at: row.started_at as number,
      completed_at: row.completed_at as number | null,
      total_tasks: row.total_tasks as number,
      completed_tasks: row.completed_tasks as number,
      failed_tasks: row.failed_tasks as number,
      alarm_count: row.alarm_count as number,
      status: row.status as string,
    };
  }

  async getTimingAnalysis(testId: string, workMs: number = 100): Promise<{
    totalWorkers: number;
    alarms: {
      alarmNum: number;
      workerCount: number;
      rpcSentAt: number;
      processedAtMin: number;
      processedAtMax: number;
      processedAtSpread: number;
      completedAtMin: number;
      completedAtMax: number;
      completedAtSpread: number;
    }[];
    overallProcessedSpread: number;
    analysis: {
      avgProcessedSpread: number;
      avgBatchSize: number;
      theoreticalSequentialMs: number;
      actualSpreadMs: number;
      estimatedParallelism: string;
      verdict: string;
    };
  }> {
    const timings = this.sql
      .exec(`SELECT * FROM worker_timings WHERE test_id = ? ORDER BY alarm_num, processed_at`, testId)
      .toArray() as {
        worker_id: number;
        alarm_num: number;
        rpc_sent_at: number;
        processed_at: number;
        completed_at: number;
      }[];

    if (timings.length === 0) {
      return {
        totalWorkers: 0,
        alarms: [],
        overallProcessedSpread: 0,
        analysis: {
          avgProcessedSpread: 0,
          avgBatchSize: 0,
          theoreticalSequentialMs: 0,
          actualSpreadMs: 0,
          estimatedParallelism: "N/A",
          verdict: "No data",
        },
      };
    }

    // Group by alarm
    const byAlarm = new Map<number, typeof timings>();
    for (const t of timings) {
      const arr = byAlarm.get(t.alarm_num) ?? [];
      arr.push(t);
      byAlarm.set(t.alarm_num, arr);
    }

    const alarms = Array.from(byAlarm.entries())
      .sort((a, b) => a[0] - b[0])
      .map(([alarmNum, workers]) => {
        const processedAts = workers.map((w) => w.processed_at);
        const completedAts = workers.map((w) => w.completed_at);
        return {
          alarmNum,
          workerCount: workers.length,
          rpcSentAt: workers[0].rpc_sent_at,
          processedAtMin: Math.min(...processedAts),
          processedAtMax: Math.max(...processedAts),
          processedAtSpread: Math.max(...processedAts) - Math.min(...processedAts),
          completedAtMin: Math.min(...completedAts),
          completedAtMax: Math.max(...completedAts),
          completedAtSpread: Math.max(...completedAts) - Math.min(...completedAts),
        };
      });

    const allProcessedAts = timings.map((t) => t.processed_at);
    const overallProcessedSpread = Math.max(...allProcessedAts) - Math.min(...allProcessedAts);

    // Calculate concurrency metrics
    const avgSpread = alarms.reduce((sum, a) => sum + a.processedAtSpread, 0) / alarms.length;
    const avgBatchSize = alarms.reduce((sum, a) => sum + a.workerCount, 0) / alarms.length;

    // If sequential: batchSize workers × workMs = theoretical time
    // Actual spread shows real parallelism
    const theoreticalSequential = avgBatchSize * Math.max(workMs, 1);
    const actualParallelism = theoreticalSequential / Math.max(avgSpread, 1);

    return {
      totalWorkers: timings.length,
      alarms,
      overallProcessedSpread,
      analysis: {
        avgProcessedSpread: Math.round(avgSpread),
        avgBatchSize: Math.round(avgBatchSize),
        theoreticalSequentialMs: theoreticalSequential,
        actualSpreadMs: Math.round(avgSpread),
        estimatedParallelism: `${actualParallelism.toFixed(1)}x`,
        verdict: avgSpread < theoreticalSequential
          ? `Concurrent: ${avgBatchSize} workers processed in ${Math.round(avgSpread)}ms (sequential would be ${theoreticalSequential}ms)`
          : `Sequential: spread ${Math.round(avgSpread)}ms >= theoretical ${theoreticalSequential}ms`
      }
    };
  }

  private retryStuckTasks(testId: string, stuckThresholdMs: number): void {
    const threshold = Date.now() - stuckThresholdMs;
    this.sql.exec(
      `UPDATE tasks SET status = 'pending', executed_at = NULL WHERE test_id = ? AND status = 'executing' AND executed_at < ?`,
      testId,
      threshold
    );
  }

  async alarm(): Promise<void> {
    const testId = await this.ctx.storage.get<string>("current_test_id");
    if (!testId) return;

    const batchSize = (await this.ctx.storage.get<number>("batch_size")) ?? 100;
    const workMs = (await this.ctx.storage.get<number>("work_ms")) ?? 0;
    const alarmNum = ((await this.ctx.storage.get<number>("alarm_num")) ?? 0) + 1;
    await this.ctx.storage.put("alarm_num", alarmNum);

    // Retry stuck tasks (executing > 10s without completion)
    this.retryStuckTasks(testId, 10000);

    // Fetch next batch
    const batch = this.sql
      .exec(
        `SELECT id, worker_id FROM tasks WHERE test_id = ? AND status = 'pending' LIMIT ?`,
        testId,
        batchSize
      )
      .toArray() as Pick<DispatchTask, "id" | "worker_id">[];

    if (batch.length === 0) {
      // Check if all done
      const pendingResult = this.sql
        .exec(
          `SELECT COUNT(*) as c FROM tasks WHERE test_id = ? AND status IN ('pending', 'executing')`,
          testId
        )
        .toArray();
      const pending = (pendingResult[0] as { c: number }).c;

      if (pending === 0) {
        // Complete
        this.sql.exec(
          `UPDATE test_results SET status = 'completed', completed_at = ? WHERE test_id = ?`,
          Date.now(),
          testId
        );
        await this.ctx.storage.delete("current_test_id");
        return;
      }
      // Safety net - retry in 5s
      await this.ctx.storage.setAlarm(Date.now() + 5000);
      return;
    }

    // Mark executing
    const now = Date.now();
    for (const task of batch) {
      this.sql.exec(
        `UPDATE tasks SET status = 'executing', executed_at = ? WHERE id = ?`,
        now,
        task.id
      );
    }

    // Fire all RPCs in parallel
    const rpcSentAt = Date.now();
    const results = await Promise.allSettled(
      batch.map(async (task) => {
        const stub = this.env.WORKER.get(this.env.WORKER.idFromName(`worker-${task.worker_id}`));
        const result = await stub.ack(task.worker_id, workMs);
        return { task, result };
      })
    );

    // Process results
    for (const r of results) {
      if (r.status === "fulfilled") {
        const { task, result } = r.value;
        this.sql.exec(`DELETE FROM tasks WHERE id = ?`, task.id);
        this.sql.exec(
          `UPDATE test_results SET completed_tasks = completed_tasks + 1 WHERE test_id = ?`,
          testId
        );
        this.sql.exec(
          `INSERT INTO worker_timings (test_id, worker_id, alarm_num, rpc_sent_at, processed_at, completed_at) VALUES (?, ?, ?, ?, ?, ?)`,
          testId,
          task.worker_id,
          alarmNum,
          rpcSentAt,
          result.processedAt,
          result.completedAt
        );
      } else {
        const task = batch.find((t) => t.id === (r as PromiseRejectedResult).reason?.taskId) ?? batch[0];
        this.sql.exec(`UPDATE tasks SET status = 'failed', error = ? WHERE id = ?`, String(r.reason), task.id);
        this.sql.exec(
          `UPDATE test_results SET failed_tasks = failed_tasks + 1 WHERE test_id = ?`,
          testId
        );
      }
    }

    // Increment alarm count
    this.sql.exec(`UPDATE test_results SET alarm_count = alarm_count + 1 WHERE test_id = ?`, testId);

    // Schedule next
    const remainingResult = this.sql
      .exec(`SELECT COUNT(*) as c FROM tasks WHERE test_id = ? AND status = 'pending'`, testId)
      .toArray();
    const remaining = (remainingResult[0] as { c: number }).c;

    if (remaining > 0) {
      await this.ctx.storage.setAlarm(Date.now() + 1); // Near-immediate
    } else {
      await this.ctx.storage.setAlarm(Date.now() + 5000); // Safety net for executing tasks
    }
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

Queue (N concurrent calls to singleton DO, each enqueues one message):
  /test/queue?calls=N&timeout=MS
    - Enqueue: N concurrent RPC calls to singleton DO, each calls queue.send()
    - Consumer: processes messages, calls singleton DO for 100-500ms random work
    - Measures enqueue latency (via runConcurrentTest queueDelay)
    - Measures e2e latency (enqueue -> consumer complete)
    - Reports batch analysis (how CF batched the messages)
    - Consumer config: max_batch_size=50, max_concurrency=10

Dispatch (singleton DO notifies N worker DOs via recursive alarms):
  /test/dispatch?tasks=2000&batch=100&work=100
    - Start test: seeds N tasks, triggers alarm chain
    - work=MS adds simulated work delay in each worker DO
    - Each alarm processes batch tasks, then schedules next alarm
    - Bypasses 1000 sub-request limit via alarm chaining
  /test/dispatch/status?testId=xxx
    - Get current test status
  /test/dispatch/timings?testId=xxx
    - Get concurrency analysis (processedAt spread per alarm batch)
  /test/dispatch/poll?testId=xxx&timeout=60000
    - Poll until completion or timeout
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

    // Queue test - N concurrent calls to singleton DO, each enqueues one message
    // Measures: enqueue latency (via runConcurrentTest) + e2e latency (via polling)
    if (url.pathname === "/test/queue") {
      const timeout = parseInt(url.searchParams.get("timeout") || "30000");
      const testId = crypto.randomUUID();

      // Get coordinator and start test
      const queueCoordinator = env.QUEUE_COORDINATOR.get(env.QUEUE_COORDINATOR.idFromName("queue-coordinator"));
      await queueCoordinator.startTest(testId, calls);

      // Get singleton DO that will enqueue messages
      const enqueuerStub = env.WORKER.get(env.WORKER.idFromName("queue-enqueuer"));

      // N concurrent calls to singleton DO, each enqueues one message
      const enqueueResult = await runConcurrentTest(calls, 0, true, async (_workerId, _delayMs, index) => {
        return enqueuerStub.enqueueMessage(testId, index);
      });

      // Poll for e2e results with timeout
      const pollStart = Date.now();
      let e2eData: QueueTestResult["e2e"] | undefined;

      while (Date.now() - pollStart < timeout) {
        const status = await queueCoordinator.getResults(testId);
        if (status.received >= status.expected) {
          // All messages processed - compute e2e analysis
          const results = status.results;
          const latencies = results.map((r) => r.completedAt - r.enqueuedAt);
          const workDurations = results.map((r) => r.workDuration);

          // Group by batch
          const batchMap = new Map<string, number[]>();
          for (const r of results) {
            const existing = batchMap.get(r.batchId) ?? [];
            existing.push(r.messageId);
            batchMap.set(r.batchId, existing);
          }
          const batches = Array.from(batchMap.entries()).map(([batchId, messageIds]) => ({
            batchId,
            size: messageIds.length,
            messageIds: messageIds.sort((a, b) => a - b),
          }));

          e2eData = {
            results,
            analysis: {
              minLatency: Math.min(...latencies),
              maxLatency: Math.max(...latencies),
              avgLatency: latencies.reduce((a, b) => a + b, 0) / latencies.length,
              minWorkDuration: Math.min(...workDurations),
              maxWorkDuration: Math.max(...workDurations),
              avgWorkDuration: workDurations.reduce((a, b) => a + b, 0) / workDurations.length,
            },
            batches,
          };
          break;
        }
        await new Promise((resolve) => setTimeout(resolve, 100));
      }

      // Cleanup
      await queueCoordinator.cleanup(testId);

      const result: QueueTestResult = {
        testId,
        totalMessages: calls,
        enqueue: {
          totalDuration: enqueueResult.totalDuration,
          results: enqueueResult.results.map((r) => ({
            messageId: r.workerId,
            startTime: r.startTime,
            endTime: r.endTime,
            duration: r.duration,
          })),
          analysis: {
            minDuration: enqueueResult.queueAnalysis.minQueueDelay,
            maxDuration: enqueueResult.queueAnalysis.maxQueueDelay,
            avgDuration: enqueueResult.queueAnalysis.avgQueueDelay,
          },
        },
        e2e: e2eData,
      };

      return Response.json(result);
    }

    // Dispatch test - singleton DO notifies N worker DOs via recursive alarms
    if (url.pathname === "/test/dispatch") {
      const tasks = parseInt(url.searchParams.get("tasks") || "2000");
      const batch = parseInt(url.searchParams.get("batch") || "100");
      const workMs = parseInt(url.searchParams.get("work") || "0");
      const dispatcher = env.DISPATCHER.get(env.DISPATCHER.idFromName("dispatcher"));
      const result = await dispatcher.startTest(tasks, batch, workMs);
      return Response.json(result);
    }

    if (url.pathname === "/test/dispatch/status") {
      const testId = url.searchParams.get("testId");
      if (!testId) {
        return new Response("Missing testId parameter", { status: 400 });
      }
      const dispatcher = env.DISPATCHER.get(env.DISPATCHER.idFromName("dispatcher"));
      const result = await dispatcher.getTestStatus(testId);
      if (!result) {
        return new Response("Test not found", { status: 404 });
      }
      return Response.json(result);
    }

    if (url.pathname === "/test/dispatch/timings") {
      const testId = url.searchParams.get("testId");
      if (!testId) {
        return new Response("Missing testId parameter", { status: 400 });
      }
      const dispatcher = env.DISPATCHER.get(env.DISPATCHER.idFromName("dispatcher"));
      const result = await dispatcher.getTimingAnalysis(testId);
      return Response.json(result);
    }

    if (url.pathname === "/test/dispatch/poll") {
      const testId = url.searchParams.get("testId");
      if (!testId) {
        return new Response("Missing testId parameter", { status: 400 });
      }
      const timeout = parseInt(url.searchParams.get("timeout") || "60000");
      const dispatcher = env.DISPATCHER.get(env.DISPATCHER.idFromName("dispatcher"));

      const pollStart = Date.now();
      while (Date.now() - pollStart < timeout) {
        const result = await dispatcher.getTestStatus(testId);
        if (!result) {
          return new Response("Test not found", { status: 404 });
        }
        if (result.status === "completed") {
          return Response.json(result);
        }
        await new Promise((resolve) => setTimeout(resolve, 500));
      }

      // Timeout - return current status
      const result = await dispatcher.getTestStatus(testId);
      return Response.json({ ...result, timedOut: true });
    }

    return new Response("Not Found", { status: 404 });
  },

  async queue(batch: MessageBatch<QueueMessage>, env: Env): Promise<void> {
    const batchId = crypto.randomUUID();
    const coordinator = env.QUEUE_COORDINATOR.get(env.QUEUE_COORDINATOR.idFromName("queue-coordinator"));
    const workerStub = env.WORKER.get(env.WORKER.idFromName("worker-singleton"));

    // Process all messages in parallel to test concurrent processing within batch
    await Promise.all(
      batch.messages.map(async (msg) => {
        try {
          const { testId, messageId, enqueuedAt } = msg.body;
          const { processedAt, completedAt, workDuration } = await workerStub.doRandomWork();
          await coordinator.recordProcessed(testId, messageId, enqueuedAt, processedAt, completedAt, workDuration, batchId);
          msg.ack();
        } catch (error) {
          console.error(`Failed to process message ${msg.body.messageId}:`, error);
          msg.retry();
        }
      })
    );
  },
};
