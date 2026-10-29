import type FastSync from "../../main";
import { dump } from "../utils/helpers";

/**
 * 并发管理器
 * 用于精确控制基于 ACK 的上行同步任务并发
 */
export class ConcurrencyLimiter {
    private plugin: FastSync;
    // grant: 占用槽位后放行；abandon: 不占用槽位直接放行（用于 clear，避免重新占满槽位）
    // grant: take the slot, then resolve; abandon: resolve without taking a slot (used by clear)
    private queue: { key: string; priority: number; grant: () => void; abandon: () => void }[] = [];
    private activeKeys: Set<string> = new Set();
    
    // 针对 FIFO 类型的 ACK（如重命名消息），记录其对应的 Key 顺序
    private fifoKeys: string[] = [];

    constructor(plugin: FastSync) {
        this.plugin = plugin;
    }

    /**
     * 等待并获取一个并发槽位
     * @param key 任务标识（通常是文件路径，或者是生成的随机 ID）
     * @param isFifo 是否是 FIFO 类型的 ACK (ACK 中不带 path)
     * @param priority 优先级（数字越大优先级越高，用于实现先上传后下载等逻辑）
     */
    public async waitForSlot(key: string, isFifo: boolean = false, priority: number = 0): Promise<void> {
        if (!this.plugin.settings.concurrencyControlEnabled) {
            return;
        }

        if (this.activeKeys.size < this.plugin.settings.maxConcurrentUploads) {
            this.activeKeys.add(key);
            if (isFifo) this.fifoKeys.push(key);
            dump(`Concurrency: Slot acquired immediately for ${key} (Priority: ${priority}). Active: ${this.activeKeys.size}`);
            return;
        }

        return new Promise((resolve) => {
            dump(`Concurrency: Queueing task ${key} (Priority: ${priority}). Current active: ${this.activeKeys.size}`);
            this.queue.push({
                key,
                priority,
                grant: () => {
                    this.activeKeys.add(key);
                    if (isFifo) this.fifoKeys.push(key);
                    dump(`Concurrency: Slot acquired from queue for ${key}. Active: ${this.activeKeys.size}`);
                    resolve();
                },
                abandon: () => {
                    dump(`Concurrency: Task ${key} released from queue without a slot.`);
                    resolve();
                }
            });
            // 按照优先级倒序排序（优先级数值越大的越靠前）
            this.queue.sort((a, b) => b.priority - a.priority);
        });
    }

    /**
     * 释放指定任务的并发槽位
     * @param key 任务标识
     */
    public releaseSlot(key: string): void {
        if (!this.plugin.settings.concurrencyControlEnabled) {
            return;
        }

        if (this.activeKeys.has(key)) {
            this.activeKeys.delete(key);
            // 同时从 FIFO 队列中移除该 key (如果存在)
            const fifoIndex = this.fifoKeys.indexOf(key);
            if (fifoIndex !== -1) {
                this.fifoKeys.splice(fifoIndex, 1);
            }
            
            dump(`Concurrency: Slot released for ${key}. Remaining active: ${this.activeKeys.size}`);
            this.processQueue();
        } else {
            // dump(`Concurrency: Skip release, key not active: ${key}`);
        }
    }

    /**
     * 针对没有带 path 的 ACK，释放最早的一个 FIFO 槽位
     */
    public releaseFifoSlot(): void {
        if (!this.plugin.settings.concurrencyControlEnabled) {
            return;
        }

        const key = this.fifoKeys.shift();
        if (key) {
            this.activeKeys.delete(key);
            dump(`Concurrency: FIFO slot released for ${key}. Remaining active: ${this.activeKeys.size}`);
            this.processQueue();
        }
    }

    /**
     * 处理等待队列
     */
    private processQueue(): void {
        const activeCount = this.activeKeys.size;
        const queueCount = this.queue.length;
        if (queueCount > 0) {
            dump(`Concurrency: Processing queue. Active: ${activeCount}, Queued: ${queueCount}`);
        }
        while (this.activeKeys.size < this.plugin.settings.maxConcurrentUploads && this.queue.length > 0) {
            const next = this.queue.shift();
            if (next) {
                dump(`Concurrency: Resuming task ${next.key} from queue.`);
                next.grant();
            }
        }
    }

    /**
     * 清空所有并发状态（通常用于断网或重连）
     */
    public clear(): void {
        dump(`Concurrency: Clearing all ${this.activeKeys.size} active tasks and ${this.queue.length} queued tasks.`);
        this.activeKeys.clear();
        this.fifoKeys = [];
        // 放行所有正在等待的 Promise，避免排队任务永久悬挂（调用方在 withLock 内，
        // resolve 后任务会因连接已断开而自然失败，走 catch/finally 释放锁）
        const pending = this.queue;
        this.queue = [];
        // 注意：必须放弃（不占槽位）而不是授予，否则会把刚清空的 activeKeys 重新占满
        // Note: abandon (take no slot) rather than grant, otherwise activeKeys would be re-filled right after clearing
        for (const item of pending) {
            item.abandon();
        }
    }

    /**
     * 重连授权成功后重置：丢弃所有活跃槽位，并按上限放行排队任务
     * 断线期间获取槽位的任务，其消息会被 Send 丢弃，永远不会收到 ACK，因此需要在重连后回收
     * Reset after a successful (re)authorization: drop all active slots and grant queued waiters up to the limit.
     * Tasks that took a slot while disconnected had their messages dropped by Send and will never get an ACK.
     */
    public resetActive(): void {
        dump(`Concurrency: Reset ${this.activeKeys.size} active slots, ${this.queue.length} queued tasks.`);
        this.activeKeys.clear();
        this.fifoKeys = [];
        this.processQueue();
    }

}
