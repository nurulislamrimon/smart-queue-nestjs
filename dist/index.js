'use strict';

var common = require('@nestjs/common');
var bullmq = require('bullmq');

var __getOwnPropDesc = Object.getOwnPropertyDescriptor;
var __decorateClass = (decorators, target, key, kind) => {
  var result = kind > 1 ? void 0 : kind ? __getOwnPropDesc(target, key) : target;
  for (var i = decorators.length - 1, decorator; i >= 0; i--)
    if (decorator = decorators[i])
      result = (decorator(result)) || result;
  return result;
};
var __decorateParam = (index, decorator) => (target, key) => decorator(target, key, index);

// src/interfaces/queue-options.interface.ts
var SMART_QUEUE_MODULE_OPTIONS = "SMART_QUEUE_MODULE_OPTIONS";
var QUEUE_SERVICE_TOKEN = "QUEUE_SERVICE";
var QUEUE_REGISTRY_TOKEN = "QUEUE_REGISTRY";
var QUEUE_METADATA_KEY = "smart-queue:queue";
var PROCESSOR_METADATA_KEY = "smart-queue:processor";
var PROCESS_HANDLER_METADATA_KEY = "smart-queue:process";
function Queue(name) {
  return (target) => {
    common.SetMetadata(QUEUE_METADATA_KEY, { name })(target);
  };
}
function Processor(queue, options) {
  return (target) => {
    common.SetMetadata(PROCESSOR_METADATA_KEY, { queue, concurrency: options?.concurrency })(target);
  };
}
function Process(name, options) {
  return (target, propertyKey, descriptor) => {
    common.SetMetadata(PROCESS_HANDLER_METADATA_KEY, {
      name,
      options
    })(target, propertyKey, descriptor);
    return descriptor;
  };
}
function getQueueMetadata(target) {
  return Reflect.getMetadata(QUEUE_METADATA_KEY, target);
}
function getProcessorMetadata(target) {
  return Reflect.getMetadata(PROCESSOR_METADATA_KEY, target);
}
function getProcessHandlerMetadata(target, propertyKey) {
  return Reflect.getMetadata(PROCESS_HANDLER_METADATA_KEY, target, propertyKey);
}
exports.QueueRegistry = class QueueRegistry {
  constructor(defaultOptions) {
    this.logger = new common.Logger(exports.QueueRegistry.name);
    this.queues = /* @__PURE__ */ new Map();
    this.workers = /* @__PURE__ */ new Map();
    this.defaultOptions = defaultOptions || {};
  }
  getConnectionOptions() {
    const connection = this.defaultOptions.connection || {};
    return {
      host: connection.host || "localhost",
      port: connection.port || 6379,
      db: connection.db || 0,
      password: connection.password
    };
  }
  getOrCreateQueue(name, options) {
    const queueName = this.getPrefixedQueueName(name);
    if (this.queues.has(queueName)) {
      return this.queues.get(queueName).queue;
    }
    const queueOptions = {
      connection: this.getConnectionOptions(),
      defaultJobOptions: {
        ...this.defaultOptions.defaultJobOptions,
        ...options?.defaultJobOptions
      }
    };
    const queue = new bullmq.Queue(queueName, queueOptions);
    const queueEvents = new bullmq.QueueEvents(queueName, {
      connection: this.getConnectionOptions()
    });
    this.queues.set(queueName, { queue, queueEvents });
    this.logger.log(`Queue created: ${queueName}`);
    return queue;
  }
  getQueue(name) {
    const queueName = this.getPrefixedQueueName(name);
    return this.queues.get(queueName)?.queue;
  }
  getQueueEvents(name) {
    const queueName = this.getPrefixedQueueName(name);
    return this.queues.get(queueName)?.queueEvents;
  }
  createWorker(name, processor, options) {
    const queueName = this.getPrefixedQueueName(name);
    const workerKey = `${queueName}-${options?.concurrency || 1}`;
    if (this.workers.has(workerKey)) {
      return this.workers.get(workerKey).worker;
    }
    const workerOptions = {
      connection: this.getConnectionOptions(),
      concurrency: options?.concurrency || 1,
      limiter: options?.limiter,
      lockDuration: options?.lockDuration,
      lockRenewTime: options?.lockRenewTime,
      maxStalledCount: options?.maxStalledCount,
      stalledInterval: options?.stalledInterval
    };
    const worker = new bullmq.Worker(queueName, async (job) => {
      try {
        const jobId = job.id || "unknown";
        const result = await processor({ id: jobId, data: job.data });
        return result;
      } catch (error) {
        const retryStrategy = options?.retryStrategy;
        if (retryStrategy) {
          const shouldRetry = job.attemptsMade < retryStrategy.attempts;
          if (shouldRetry) {
            let delay;
            if (typeof retryStrategy.backoff === "number") {
              delay = retryStrategy.backoff;
            } else if (retryStrategy.backoff?.type === "exponential") {
              delay = Math.pow(2, job.attemptsMade) * (retryStrategy.backoff.delay || 1e3);
            } else if (retryStrategy.backoff?.type === "fixed") {
              delay = retryStrategy.backoff.delay || 1e3;
            }
            if (delay) {
              await job.updateProgress(100);
              throw new Error(`Job failed, will retry after ${delay}ms`);
            }
          }
        }
        throw error;
      }
    }, workerOptions);
    worker.on("completed", (job) => {
      this.logger.debug(`Job ${job.id} completed in queue ${queueName}`);
    });
    worker.on("failed", (job, error) => {
      this.logger.error(`Job ${job?.id} failed in queue ${queueName}: ${error.message}`);
    });
    worker.on("stalled", (jobId) => {
      this.logger.warn(`Job ${jobId} stalled in queue ${queueName}`);
    });
    this.workers.set(workerKey, { worker, queueName });
    this.logger.log(`Worker created for queue: ${queueName}`);
    return worker;
  }
  registerEventListeners(queueName, handlers) {
    const queueEvents = this.getQueueEvents(queueName);
    if (!queueEvents) {
      this.logger.warn(`Queue events not found for queue: ${queueName}`);
      return;
    }
    if (handlers.completed) {
      queueEvents.on("completed", ({ jobId, returnvalue }) => {
        handlers.completed(jobId, returnvalue);
      });
    }
    if (handlers.failed) {
      queueEvents.on("failed", ({ jobId, failedReason }) => {
        handlers.failed(jobId, new Error(failedReason ?? "Unknown error"));
      });
    }
    if (handlers.progress) {
      queueEvents.on("progress", (data) => {
        handlers.progress(data.jobId, data.data);
      });
    }
    if (handlers.waiting) {
      queueEvents.on("waiting", ({ jobId }) => {
        handlers.waiting(jobId);
      });
    }
    if (handlers.stalled) {
      queueEvents.on("stalled", ({ jobId }) => {
        handlers.stalled(jobId);
      });
    }
  }
  async closeQueue(name) {
    const queueName = this.getPrefixedQueueName(name);
    const registered = this.queues.get(queueName);
    if (registered) {
      await registered.queueEvents.close();
      await registered.queue.close();
      this.queues.delete(queueName);
      this.logger.log(`Queue closed: ${queueName}`);
    }
  }
  async closeWorker(name) {
    const queueName = this.getPrefixedQueueName(name);
    for (const [key, worker] of this.workers.entries()) {
      if (worker.queueName === queueName) {
        await worker.worker.close();
        this.workers.delete(key);
        this.logger.log(`Worker closed for queue: ${queueName}`);
        break;
      }
    }
  }
  async closeAll() {
    for (const [name] of this.queues) {
      await this.closeQueue(name);
    }
    for (const [key] of this.workers) {
      const worker = this.workers.get(key);
      if (worker) {
        await worker.worker.close();
        this.workers.delete(key);
      }
    }
    this.logger.log("All queues and workers closed");
  }
  async onModuleDestroy() {
    await this.closeAll();
  }
  getPrefixedQueueName(name) {
    const prefix = this.defaultOptions.connection?.keyPrefix || "smart-queue";
    return `${prefix}:${name}`;
  }
};
exports.QueueRegistry = __decorateClass([
  common.Injectable()
], exports.QueueRegistry);
exports.BullMQAdapter = class BullMQAdapter {
  constructor(registry) {
    this.registry = registry;
    this.logger = new common.Logger(exports.BullMQAdapter.name);
  }
  getQueue(name) {
    const queue = this.registry.getQueue(name);
    if (!queue) {
      return this.registry.getOrCreateQueue(name);
    }
    return queue;
  }
  async addJob(queueName, name, data, options) {
    const queue = this.getQueue(queueName);
    const job = await queue.add(name, data, {
      jobId: options?.jobId,
      priority: options?.priority,
      delay: options?.delay,
      attempts: options?.attempts,
      backoff: options?.backoff,
      lifo: options?.lifo,
      timeout: options?.timeout,
      removeOnComplete: options?.removeOnComplete ?? true,
      removeOnFail: options?.removeOnFail ?? 100,
      repeatJobKey: options?.repeatJobKey
    });
    this.logger.debug(`Job added to queue ${queueName}: ${job.id}`);
    return job;
  }
  async addDelayedJob(queueName, name, data, delay, options) {
    return this.addJob(queueName, name, data, {
      ...options,
      delay
    });
  }
  async addRepeatableJob(queueName, name, data, cron, options) {
    const queue = this.getQueue(queueName);
    const repeatOptions = {
      pattern: cron,
      tz: options?.tz,
      startDate: options?.startDate ? new Date(options.startDate) : void 0,
      endDate: options?.endDate ? new Date(options.endDate) : void 0,
      limit: options?.limit
    };
    const job = await queue.add(name, data, {
      jobId: options?.jobId,
      repeat: repeatOptions
    });
    this.logger.debug(`Repeatable job added to queue ${queueName} with cron: ${cron}`);
    return job;
  }
  async removeJob(queueName, jobId) {
    const queue = this.getQueue(queueName);
    const job = await queue.getJob(jobId);
    if (job) {
      await job.remove();
      this.logger.debug(`Job ${jobId} removed from queue ${queueName}`);
    }
  }
  async getJob(queueName, jobId) {
    const queue = this.getQueue(queueName);
    const job = await queue.getJob(jobId);
    return job ?? null;
  }
  async getMetrics(queueName) {
    const queue = this.getQueue(queueName);
    const counts = await queue.getJobCounts("waiting", "active", "completed", "failed", "delayed", "paused");
    return {
      waiting: counts.waiting || 0,
      active: counts.active || 0,
      completed: counts.completed || 0,
      failed: counts.failed || 0,
      delayed: counts.delayed || 0,
      paused: counts.paused || 0
    };
  }
  async pauseQueue(queueName) {
    const queue = this.getQueue(queueName);
    await queue.pause();
    this.logger.log(`Queue ${queueName} paused`);
  }
  async resumeQueue(queueName) {
    const queue = this.getQueue(queueName);
    await queue.resume();
    this.logger.log(`Queue ${queueName} resumed`);
  }
  async drainQueue(queueName) {
    const queue = this.getQueue(queueName);
    await queue.drain();
    this.logger.log(`Queue ${queueName} drained`);
  }
  async cleanQueue(queueName, grace, status) {
    const queue = this.getQueue(queueName);
    const jobs = await queue.clean(grace, 100, status);
    this.logger.debug(`Cleaned ${jobs.length} jobs from queue ${queueName}`);
    return jobs;
  }
  createWorker(queueName, processor, options) {
    this.registry.createWorker(queueName, processor, options);
  }
  registerEventListeners(queueName, handlers) {
    this.registry.registerEventListeners(queueName, handlers);
  }
  async removeRepeatableJob(queueName, jobKey) {
    const queue = this.getQueue(queueName);
    const removed = await queue.removeRepeatable(jobKey);
    this.logger.debug(`Removed repeatable job ${jobKey} from queue ${queueName}`);
    return removed;
  }
  async getRepeatableJobs(queueName) {
    const queue = this.getQueue(queueName);
    return queue.getRepeatableJobs();
  }
  async closeQueue(queueName) {
    await this.registry.closeQueue(queueName);
  }
  async closeWorker(queueName) {
    await this.registry.closeWorker(queueName);
  }
};
exports.BullMQAdapter = __decorateClass([
  common.Injectable()
], exports.BullMQAdapter);
exports.QueueService = class QueueService {
  constructor(adapter, moduleOptions) {
    this.adapter = adapter;
    this.moduleOptions = moduleOptions;
    this.logger = new common.Logger(exports.QueueService.name);
    this.defaultQueueOptions = moduleOptions || {};
  }
  async add(queueName, name, data, options) {
    const job = await this.adapter.addJob(queueName, name, data, options);
    this.logger.log(`Job added to queue '${queueName}': ${name}`);
    return job;
  }
  async delay(queueName, name, data, delay, options) {
    return this.adapter.addDelayedJob(queueName, name, data, delay, options);
  }
  async repeat(queueName, name, data, cron, options) {
    return this.adapter.addRepeatableJob(queueName, name, data, cron, options);
  }
  async remove(queueName, jobId) {
    await this.adapter.removeJob(queueName, jobId);
  }
  async getJob(queueName, jobId) {
    return this.adapter.getJob(queueName, jobId);
  }
  async getMetrics(queueName) {
    return this.adapter.getMetrics(queueName);
  }
  async pause(queueName) {
    await this.adapter.pauseQueue(queueName);
  }
  async resume(queueName) {
    await this.adapter.resumeQueue(queueName);
  }
  async drain(queueName) {
    await this.adapter.drainQueue(queueName);
  }
  async clean(queueName, grace = 5e3, status) {
    return this.adapter.cleanQueue(queueName, grace, status);
  }
  async removeRepeatable(queueName, jobKey) {
    return this.adapter.removeRepeatableJob(queueName, jobKey);
  }
  async getRepeatableJobs(queueName) {
    return this.adapter.getRepeatableJobs(queueName);
  }
  worker(queueName, processor, options) {
    this.adapter.createWorker(queueName, processor, options);
  }
  on(queueName, event, handler) {
    const handlers = {};
    switch (event) {
      case "completed":
        handlers.completed = handler;
        break;
      case "failed":
        handlers.failed = handler;
        break;
      case "progress":
        handlers.progress = handler;
        break;
      case "waiting":
        handlers.waiting = handler;
        break;
      case "stalled":
        handlers.stalled = handler;
        break;
    }
    this.adapter.registerEventListeners(queueName, handlers);
  }
  async close(queueName) {
    await this.adapter.closeQueue(queueName);
  }
};
exports.QueueService = __decorateClass([
  common.Injectable(),
  __decorateParam(1, common.Inject(SMART_QUEUE_MODULE_OPTIONS))
], exports.QueueService);
exports.SmartQueueModule = class SmartQueueModule {
  constructor() {
  }
  static forRoot(options) {
    const providers = this.createProviders(options);
    return {
      module: exports.SmartQueueModule,
      providers,
      exports: [exports.QueueRegistry, exports.BullMQAdapter, exports.QueueService]
    };
  }
  static forRootAsync(options) {
    const providers = this.createAsyncProviders(options);
    return {
      module: exports.SmartQueueModule,
      imports: options.imports || [],
      providers,
      exports: [exports.QueueRegistry, exports.BullMQAdapter, exports.QueueService]
    };
  }
  static createProviders(options) {
    return [
      {
        provide: SMART_QUEUE_MODULE_OPTIONS,
        useValue: options || {}
      },
      exports.QueueRegistry,
      exports.BullMQAdapter,
      exports.QueueService
    ];
  }
  static createAsyncProviders(options) {
    const providers = [
      {
        provide: SMART_QUEUE_MODULE_OPTIONS,
        useFactory: (factory) => {
          return factory.createSmartQueueOptions();
        },
        inject: [options.useClass]
      },
      {
        provide: options.useClass,
        useClass: options.useClass
      },
      exports.QueueRegistry,
      exports.BullMQAdapter,
      exports.QueueService
    ];
    return providers;
  }
  configure(consumer) {
  }
};
exports.SmartQueueModule = __decorateClass([
  common.Global(),
  common.Module({})
], exports.SmartQueueModule);

exports.PROCESSOR_METADATA_KEY = PROCESSOR_METADATA_KEY;
exports.PROCESS_HANDLER_METADATA_KEY = PROCESS_HANDLER_METADATA_KEY;
exports.Process = Process;
exports.Processor = Processor;
exports.QUEUE_METADATA_KEY = QUEUE_METADATA_KEY;
exports.QUEUE_REGISTRY_TOKEN = QUEUE_REGISTRY_TOKEN;
exports.QUEUE_SERVICE_TOKEN = QUEUE_SERVICE_TOKEN;
exports.Queue = Queue;
exports.SMART_QUEUE_MODULE_OPTIONS = SMART_QUEUE_MODULE_OPTIONS;
exports.getProcessHandlerMetadata = getProcessHandlerMetadata;
exports.getProcessorMetadata = getProcessorMetadata;
exports.getQueueMetadata = getQueueMetadata;
//# sourceMappingURL=index.js.map
//# sourceMappingURL=index.js.map