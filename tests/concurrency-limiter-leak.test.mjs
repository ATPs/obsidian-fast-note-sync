import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";
import ts from "typescript";

const root = path.resolve(import.meta.dirname, "..");
const sourcePath = path.join(root, "src", "lib", "sync", "concurrency_limiter.ts");
let source = fs.readFileSync(sourcePath, "utf8");

// 去掉对 obsidian 插件/helpers 的依赖，只保留限流器本身
// Strip imports (plugin type / helpers) so only the limiter itself is compiled
source = source
  .replace(/^import type FastSync.*$/m, "")
  .replace(/^import \{ dump \} from .*$/m, "const dump = (..._args) => undefined;")
  .replace(/private plugin: FastSync;/, "private plugin: any;")
  .replace(/constructor\(plugin: FastSync\)/, "constructor(plugin: any)");

const outDir = fs.mkdtempSync(path.join(os.tmpdir(), "fns-limiter-test-"));
const outFile = path.join(outDir, "concurrency_limiter.mjs");
fs.writeFileSync(
  outFile,
  ts.transpileModule(source, {
    compilerOptions: { module: ts.ModuleKind.ESNext, target: ts.ScriptTarget.ES2020 },
    fileName: sourcePath,
  }).outputText,
);
const { ConcurrencyLimiter } = await import(pathToFileURL(outFile).href);

const makeLimiter = (max, enabled = true) =>
  new ConcurrencyLimiter({ settings: { concurrencyControlEnabled: enabled, maxConcurrentUploads: max } });
const active = (l) => l.activeKeys.size;
const flush = () => new Promise((r) => setTimeout(r, 0));
const track = (p) => {
  const s = { done: false };
  p.then(() => { s.done = true; });
  return s;
};

try {
  // 正常获取 / 释放 / 排队
  // normal acquire, release and queueing
  {
    const l = makeLimiter(2);
    await l.waitForSlot("a");
    await l.waitForSlot("b");
    assert.equal(active(l), 2);
    const c = track(l.waitForSlot("c"));
    await flush();
    assert.equal(c.done, false, "third waiter must queue");
    l.releaseSlot("a");
    await flush();
    assert.equal(c.done, true, "queued waiter granted on release");
    assert.deepEqual([...l.activeKeys].sort(), ["b", "c"]);
    l.releaseSlot("unknown");
    assert.equal(active(l), 2, "release of unknown key is a no-op");
    l.releaseSlot("b");
    l.releaseSlot("c");
    assert.equal(active(l), 0);
  }

  // 优先级：数值大的先被放行
  // priority: higher value is granted first
  {
    const l = makeLimiter(1);
    await l.waitForSlot("x");
    const order = [];
    const lo = l.waitForSlot("lo", false, -10).then(() => order.push("lo"));
    const hi = l.waitForSlot("hi", false, 10).then(() => order.push("hi"));
    l.releaseSlot("x");
    await flush();
    l.releaseSlot("hi");
    await Promise.all([lo, hi]);
    assert.deepEqual(order, ["hi", "lo"]);
  }

  // FIFO 释放
  // FIFO release
  {
    const l = makeLimiter(3);
    await l.waitForSlot("r1", true);
    await l.waitForSlot("r2", true);
    l.releaseFifoSlot();
    assert.deepEqual([...l.activeKeys], ["r2"]);
  }

  // 回归：clear() 放行排队任务时不得重新占用槽位
  // regression: clear() must not re-occupy slots when it resolves queued waiters
  {
    const l = makeLimiter(2);
    await l.waitForSlot("a");
    await l.waitForSlot("b");
    const queued = ["c", "d", "e"].map((k) => track(l.waitForSlot(k)));
    await flush();
    assert.ok(queued.every((s) => !s.done));
    l.clear();
    await flush();
    assert.ok(queued.every((s) => s.done), "queued waiters must be resolved by clear()");
    assert.equal(active(l), 0, "clear() must leave 0 active slots");
    assert.equal(l.queue.length, 0);
    assert.equal(l.fifoKeys.length, 0);
    // 被放弃的调用方稍后 release 应为 no-op，且新任务可立即获取槽位
    // abandoned callers releasing later is a no-op; new tasks acquire immediately
    l.releaseSlot("c");
    assert.equal(active(l), 0);
    const f = track(l.waitForSlot("f"));
    await flush();
    assert.equal(f.done, true);
    assert.equal(active(l), 1);
  }

  // resetActive()：丢弃活跃槽位并按上限放行排队任务
  // resetActive(): drops active slots and grants queued waiters up to the limit
  {
    const l = makeLimiter(2);
    await l.waitForSlot("a");
    await l.waitForSlot("b", true);
    const queued = ["c", "d", "e"].map((k) => track(l.waitForSlot(k)));
    await flush();
    l.resetActive();
    await flush();
    assert.deepEqual(queued.map((s) => s.done), [true, true, false], "grants exactly up to the limit");
    assert.deepEqual([...l.activeKeys].sort(), ["c", "d"]);
    assert.equal(l.fifoKeys.length, 0);
    assert.equal(l.queue.length, 1);
    // 旧任务（已被重置）的 release 是 no-op，不影响新持有者
    // stale release of a reset key is a no-op
    l.releaseSlot("a");
    assert.equal(active(l), 2);
    l.releaseSlot("c");
    await flush();
    assert.equal(queued[2].done, true);
    assert.deepEqual([...l.activeKeys].sort(), ["d", "e"]);
  }

  // resetActive() 在无排队任务时只清空
  // resetActive() with an empty queue simply clears
  {
    const l = makeLimiter(2);
    await l.waitForSlot("a");
    l.resetActive();
    assert.equal(active(l), 0);
  }

  // 关闭并发控制时不占用槽位
  // disabled concurrency control never takes slots
  {
    const l = makeLimiter(1, false);
    await l.waitForSlot("a");
    await l.waitForSlot("b");
    assert.equal(active(l), 0);
  }

  console.log("concurrency-limiter-leak tests passed");
} finally {
  fs.rmSync(outDir, { recursive: true, force: true });
}
