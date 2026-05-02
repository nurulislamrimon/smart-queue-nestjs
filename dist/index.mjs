import { Injectable, Inject, Global, Module, Optional, Logger, SetMetadata } from '@nestjs/common';
import { Queue as Queue$1, QueueEvents, Worker } from 'bullmq';

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
var QUEUE_HEALTH_INDICATOR = "QUEUE_HEALTH_INDICATOR";
var QUEUE_METADATA_KEY = "smart-queue:queue";
var PROCESSOR_METADATA_KEY = "smart-queue:processor";
var QUEUE_HANDLER_METADATA_KEY = "smart-queue:handler";
function Queue(name) {
  return SetMetadata(QUEUE_METADATA_KEY, { name });
}
function Processor(queue, options) {
  return SetMetadata(PROCESSOR_METADATA_KEY, { queue, concurrency: options?.concurrency });
}
function QueueHandler(name, options) {
  return SetMetadata(QUEUE_HANDLER_METADATA_KEY, { name, options });
}
function getQueueMetadata(target) {
  return Reflect.getMetadata(QUEUE_METADATA_KEY, target);
}
function getProcessorMetadata(target) {
  return Reflect.getMetadata(PROCESSOR_METADATA_KEY, target);
}
function getQueueHandlerMetadata(target, propertyKey) {
  return Reflect.getMetadata(QUEUE_HANDLER_METADATA_KEY, target, propertyKey);
}
var QueueRegistry = class {
  constructor(defaultOptions, metricsService) {
    this.metricsService = metricsService;
    this.logger = new Logger(QueueRegistry.name);
    this.queues = /* @__PURE__ */ new Map();
    this.workers = /* @__PURE__ */ new Map();
    this.queueConfigs = /* @__PURE__ */ new Map();
    this.jobHooks = /* @__PURE__ */ new Map();
    this.isInitialized = false;
    this.defaultOptions = defaultOptions || {};
  }
  async onModuleInit() {
    this.isInitialized = true;
    this.logger.log("QueueRegistry initialized");
  }
  getConnectionOptions() {
    const connection = this.defaultOptions.connection || {};
    const options = {
      host: connection.host || "localhost",
      port: connection.port || 6379,
      db: connection.db || 0,
      password: connection.password,
      family: connection.family,
      keyPrefix: connection.keyPrefix,
      connectTimeout: connection.connectTimeout,
      maxRetriesPerRequest: connection.maxRetriesPerRequest,
      enableOfflineQueue: connection.enableOfflineQueue
    };
    if (connection.tls?.enabled) {
      options.tls = {
        ca: connection.tls.ca,
        cert: connection.tls.cert,
        key: connection.tls.key,
        rejectUnauthorized: connection.tls.rejectUnauthorized
      };
    }
    return options;
  }
  registerQueue(name, options) {
    const queueName = this.getPrefixedQueueName(name);
    this.queueConfigs.set(queueName, { name, options });
  }
  registerDeadLetterQueue(mainQueueName, options) {
    const queueName = this.getPrefixedQueueName(mainQueueName);
    const config = this.queueConfigs.get(queueName);
    if (config) {
      config.deadLetterOptions = options;
    }
  }
  getOrCreateQueue(name) {
    const queueName = this.getPrefixedQueueName(name);
    if (this.queues.has(queueName)) {
      return this.queues.get(queueName).queue;
    }
    const connectionOptions = this.getConnectionOptions();
    const config = this.queueConfigs.get(queueName);
    const queueOptions = {
      connection: connectionOptions,
      defaultJobOptions: {
        ...this.defaultOptions.defaultJobOptions,
        ...config?.options?.defaultJobOptions
      }
    };
    const queue = new Queue$1(queueName, queueOptions);
    const queueEvents = new QueueEvents(queueName, {
      connection: connectionOptions
    });
    let deadLetterQueue;
    if (config?.deadLetterOptions?.enabled) {
      const dlqName = `${queueName}:dlq`;
      deadLetterQueue = new Queue$1(dlqName, {
        connection: connectionOptions
      });
    }
    this.queues.set(queueName, { queue, queueEvents, deadLetterQueue });
    this.logger.log(`Queue created: ${queueName}`, "QueueRegistry");
    return queue;
  }
  getQueue(name) {
    const queueName = this.getPrefixedQueueName(name);
    return this.queues.get(queueName)?.queue;
  }
  getDeadLetterQueue(name) {
    const queueName = this.getPrefixedQueueName(name);
    return this.queues.get(queueName)?.deadLetterQueue;
  }
  getQueueEvents(name) {
    const queueName = this.getPrefixedQueueName(name);
    return this.queues.get(queueName)?.queueEvents;
  }
  getRegisteredQueueNames() {
    return Array.from(this.queues.keys());
  }
  registerJobHooks(queueName, hooks) {
    const prefixedName = this.getPrefixedQueueName(queueName);
    this.jobHooks.set(prefixedName, hooks);
  }
  getJobHooks(queueName) {
    const prefixedName = this.getPrefixedQueueName(queueName);
    return this.jobHooks.get(prefixedName);
  }
  createWorker(name, processor, options) {
    if (!this.isInitialized) {
      throw new Error(
        `QueueRegistry is not initialized. Ensure SmartQueueModule.forRoot() has been called and the module has been initialized before creating workers for queue: ${name}`
      );
    }
    const queueName = this.getPrefixedQueueName(name);
    const workerKey = `${queueName}-${options?.concurrency || 1}`;
    if (this.workers.has(workerKey)) {
      this.logger.warn(
        `Worker already exists for queue ${queueName}. Returning existing worker.`
      );
      return this.workers.get(workerKey).worker;
    }
    const connectionOptions = this.getConnectionOptions();
    const workerOptions = {
      connection: connectionOptions,
      concurrency: options?.concurrency || 1,
      limiter: options?.limiter,
      lockDuration: options?.lockDuration,
      lockRenewTime: options?.lockRenewTime,
      maxStalledCount: options?.maxStalledCount,
      stalledInterval: options?.stalledInterval
    };
    const config = this.queueConfigs.get(queueName);
    const retryStrategy = options?.retryStrategy;
    const dlqOptions = config?.deadLetterOptions;
    const hooks = this.jobHooks.get(queueName);
    const wrappedProcessor = async (job) => {
      const jobId = job.id || "unknown";
      const startTime = Date.now();
      try {
        this.metricsService?.recordJobActive(queueName);
        this.logger.log(
          `Job ${jobId} started processing (attempt ${job.attemptsMade + 1})`,
          "QueueRegistry"
        );
        if (hooks?.beforeJob) {
          await hooks.beforeJob(jobId, job.data);
        }
        const result = await processor({ id: jobId, data: job.data });
        if (hooks?.afterJob) {
          await hooks.afterJob(jobId, result);
        }
        const duration = Date.now() - startTime;
        this.metricsService?.recordJobCompleted(queueName, duration);
        this.logger.log(
          `Job ${jobId} completed in ${duration}ms`,
          "QueueRegistry"
        );
        return result;
      } catch (error) {
        const attempt = job.attemptsMade || 0;
        const maxAttempts = retryStrategy?.maxAttempts || 3;
        const canRetry = retryStrategy?.enabled !== false && attempt < maxAttempts;
        if (canRetry) {
          const delay = this.calculateBackoff(attempt, retryStrategy?.backoff);
          this.logger.warn(
            `Job ${jobId} failed, will retry after ${delay}ms (attempt ${attempt + 1}/${maxAttempts})`,
            "QueueRegistry"
          );
          if (delay > 0) {
            await job.updateProgress(-1);
            const err = new Error(
              `Job failed, will retry after ${delay}ms. Attempt ${attempt + 1}/${maxAttempts}`
            );
            err.retryDelay = delay;
            throw err;
          }
        }
        this.metricsService?.recordJobFailed(queueName);
        if (hooks?.onJobFailed) {
          await hooks.onJobFailed(jobId, error);
        }
        if (dlqOptions?.enabled) {
          await this.moveToDeadLetterQueue(job, error);
        }
        this.logger.error(
          `Job ${jobId} permanently failed after ${attempt + 1} attempts`,
          error.stack,
          "QueueRegistry"
        );
        throw error;
      }
    };
    const worker = new Worker(queueName, wrappedProcessor, workerOptions);
    worker.on("completed", (job) => {
      this.logger.debug(`Job ${job.id} completed`, "QueueRegistry");
    });
    worker.on("failed", (job, error) => {
      this.logger.error(
        `Job ${job?.id} failed: ${error.message}`,
        error.stack,
        "QueueRegistry"
      );
    });
    worker.on("stalled", (jobId) => {
      this.logger.warn(`Job ${jobId} stalled`, "QueueRegistry");
    });
    this.workers.set(workerKey, { worker, queueName });
    this.logger.log(
      `Worker created for queue: ${queueName} (concurrency: ${options?.concurrency || 1})`,
      "QueueRegistry"
    );
    return worker;
  }
  calculateBackoff(attempt, backoff) {
    if (!backoff) {
      return 0;
    }
    if (typeof backoff === "number") {
      return backoff;
    }
    if (backoff.type === "exponential") {
      return Math.pow(2, attempt) * backoff.delay;
    }
    return backoff.delay;
  }
  async moveToDeadLetterQueue(job, error) {
    const queueName = this.getPrefixedQueueName(job.queueName);
    const registered = this.queues.get(queueName);
    if (!registered?.deadLetterQueue) {
      return;
    }
    const dlqJob = {
      data: job.data,
      opts: {
        jobId: `dlq-${job.id}`
      }
    };
    await registered.deadLetterQueue.add(job.name, {
      ...dlqJob.data,
      __originalJobId__: job.id,
      __failureReason__: error.message,
      __failedAt__: (/* @__PURE__ */ new Date()).toISOString()
    });
    this.logger.warn(
      `Job ${job.id} moved to dead letter queue`,
      "QueueRegistry"
    );
  }
  registerEventListeners(queueName, handlers) {
    const prefixedName = this.getPrefixedQueueName(queueName);
    const queueEvents = this.getQueueEvents(prefixedName);
    if (!queueEvents) {
      this.logger.warn(
        `Queue events not found for queue: ${queueName}`,
        "QueueRegistry"
      );
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
      if (registered.deadLetterQueue) {
        await registered.deadLetterQueue.close();
      }
      await registered.queueEvents.close();
      await registered.queue.close();
      this.queues.delete(queueName);
      this.queueConfigs.delete(queueName);
      this.jobHooks.delete(queueName);
      this.logger.log(`Queue closed: ${queueName}`, "QueueRegistry");
    }
  }
  async closeWorker(name) {
    const queueName = this.getPrefixedQueueName(name);
    for (const [key, workerEntry] of this.workers.entries()) {
      if (workerEntry.queueName === queueName) {
        await workerEntry.worker.close();
        this.workers.delete(key);
        this.logger.log(`Worker closed for queue: ${queueName}`, "QueueRegistry");
        break;
      }
    }
  }
  async closeAll() {
    this.logger.log("Closing all queues and workers...", "QueueRegistry");
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
    this.workers.clear();
    this.logger.log("All queues and workers closed", "QueueRegistry");
  }
  async onModuleDestroy() {
    await this.closeAll();
    this.logger.log("QueueRegistry destroyed", "QueueRegistry");
  }
  getPrefixedQueueName(name) {
    const prefix = this.defaultOptions.prefix || this.defaultOptions.connection?.keyPrefix || "smart-queue";
    return `${prefix}:${name}`;
  }
};
QueueRegistry = __decorateClass([
  Injectable()
], QueueRegistry);
var BullMQAdapter = class {
  constructor(registry, metricsService) {
    this.registry = registry;
    this.metricsService = metricsService;
    this.logger = new Logger(BullMQAdapter.name);
  }
  registerQueue(name, options) {
    this.registry.registerQueue(name, options);
  }
  registerDeadLetterQueue(name, options) {
    this.registry.registerDeadLetterQueue(name, options);
  }
  registerJobHooks(name, hooks) {
    this.registry.registerJobHooks(name, hooks);
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
    this.metricsService?.recordJobCreated(queueName);
    this.logger.log(
      `Job ${job.id} added to queue '${queueName}': ${name}`,
      "BullMQAdapter"
    );
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
    this.metricsService?.recordJobCreated(queueName);
    this.logger.debug(
      `Repeatable job added to queue ${queueName} with cron: ${cron}`,
      "BullMQAdapter"
    );
    return job;
  }
  async removeJob(queueName, jobId) {
    const queue = this.getQueue(queueName);
    const job = await queue.getJob(jobId);
    if (job) {
      await job.remove();
      this.logger.log(
        `Job ${jobId} removed from queue ${queueName}`,
        "BullMQAdapter"
      );
    }
  }
  async getJob(queueName, jobId) {
    const queue = this.getQueue(queueName);
    const job = await queue.getJob(jobId);
    return job ?? null;
  }
  async getQueueMetrics(queueName) {
    const queue = this.getQueue(queueName);
    const counts = await queue.getJobCounts(
      "waiting",
      "active",
      "completed",
      "failed",
      "delayed",
      "paused"
    );
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
    this.logger.log(`Queue ${queueName} paused`, "BullMQAdapter");
  }
  async resumeQueue(queueName) {
    const queue = this.getQueue(queueName);
    await queue.resume();
    this.logger.log(`Queue ${queueName} resumed`, "BullMQAdapter");
  }
  async drainQueue(queueName) {
    const queue = this.getQueue(queueName);
    await queue.drain();
    this.logger.log(`Queue ${queueName} drained`, "BullMQAdapter");
  }
  async cleanQueue(queueName, grace, status) {
    const queue = this.getQueue(queueName);
    const jobs = await queue.clean(grace, 100, status);
    this.logger.log(
      `Cleaned ${jobs.length} jobs from queue ${queueName}`,
      "BullMQAdapter"
    );
    return jobs;
  }
  createWorker(queueName, processor, options) {
    if (!this.registry) {
      throw new Error(
        "QueueRegistry is not available in BullMQAdapter. Ensure SmartQueueModule.forRoot() is properly configured."
      );
    }
    this.registry.createWorker(queueName, processor, options);
  }
  registerEventListeners(queueName, handlers) {
    this.registry.registerEventListeners(queueName, handlers);
  }
  async removeRepeatableJob(queueName, jobKey) {
    const queue = this.getQueue(queueName);
    const removed = await queue.removeRepeatable(jobKey);
    this.logger.debug(
      `Removed repeatable job ${jobKey} from queue ${queueName}`,
      "BullMQAdapter"
    );
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
  getQueueNames() {
    return this.registry.getRegisteredQueueNames();
  }
  async onModuleDestroy() {
    await this.registry.closeAll();
  }
};
BullMQAdapter = __decorateClass([
  Injectable()
], BullMQAdapter);
var QueueService = class {
  constructor(adapter, healthService, metricsService, moduleOptions) {
    this.adapter = adapter;
    this.healthService = healthService;
    this.metricsService = metricsService;
    this.logger = new Logger(QueueService.name);
    this.defaultQueueOptions = moduleOptions || {};
  }
  registerQueue(name, options) {
    this.adapter.registerQueue(name, options);
  }
  registerDeadLetterQueue(queueName, options) {
    this.adapter.registerDeadLetterQueue(queueName, options);
  }
  registerJobHooks(queueName, hooks) {
    this.adapter.registerJobHooks(queueName, hooks);
  }
  async add(queueName, name, data, options) {
    return this.adapter.addJob(queueName, name, data, options);
  }
  async addWithIdempotencyKey(queueName, name, data, idempotencyKey, options) {
    const existingJob = await this.adapter.getJob(queueName, idempotencyKey);
    if (existingJob) {
      this.logger.warn(
        `Job with idempotency key '${idempotencyKey}' already exists. Returning existing job.`
      );
      return existingJob;
    }
    return this.adapter.addJob(queueName, name, data, {
      ...options,
      jobId: idempotencyKey
    });
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
  async getQueueMetrics(queueName) {
    return this.adapter.getQueueMetrics(queueName);
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
  async checkHealth(queueName) {
    return this.healthService.checkQueueHealth(queueName);
  }
  async checkAllHealth() {
    return this.healthService.checkAllQueuesHealth();
  }
  async isHealthy(queueName) {
    return this.healthService.isHealthy(queueName);
  }
  getMetrics() {
    return this.metricsService.getPrometheusMetrics();
  }
  async close(queueName) {
    await this.adapter.closeQueue(queueName);
  }
  async onModuleDestroy() {
    await this.adapter.onModuleDestroy();
  }
};
QueueService = __decorateClass([
  Injectable(),
  __decorateParam(3, Inject(SMART_QUEUE_MODULE_OPTIONS))
], QueueService);
var QueueMetricsService = class {
  constructor() {
    this.logger = new Logger(QueueMetricsService.name);
    this.queueMetrics = /* @__PURE__ */ new Map();
  }
  recordJobCreated(queueName) {
    const metrics = this.getOrCreateQueueMetrics(queueName);
    metrics.jobsCreated++;
    this.logger.debug(
      `Job created in queue ${queueName}. Total: ${metrics.jobsCreated}`,
      "QueueMetrics"
    );
  }
  recordJobCompleted(queueName, durationMs) {
    const metrics = this.getOrCreateQueueMetrics(queueName);
    metrics.jobsCompleted++;
    metrics.jobsActive = Math.max(0, metrics.jobsActive - 1);
    if (metrics.averageDuration === 0) {
      metrics.averageDuration = durationMs;
    } else {
      metrics.averageDuration = (metrics.averageDuration * (metrics.jobsCompleted - 1) + durationMs) / metrics.jobsCompleted;
    }
    this.logger.debug(
      `Job completed in queue ${queueName}. Avg duration: ${metrics.averageDuration.toFixed(2)}ms`,
      "QueueMetrics"
    );
  }
  recordJobFailed(queueName) {
    const metrics = this.getOrCreateQueueMetrics(queueName);
    metrics.jobsFailed++;
    metrics.jobsActive = Math.max(0, metrics.jobsActive - 1);
    this.logger.debug(
      `Job failed in queue ${queueName}. Total failures: ${metrics.jobsFailed}`,
      "QueueMetrics"
    );
  }
  recordJobActive(queueName) {
    const metrics = this.getOrCreateQueueMetrics(queueName);
    metrics.jobsActive++;
    this.logger.debug(
      `Job activated in queue ${queueName}. Active: ${metrics.jobsActive}`,
      "QueueMetrics"
    );
  }
  recordJobWaiting(queueName, count) {
    const metrics = this.getOrCreateQueueMetrics(queueName);
    metrics.jobsWaiting = count;
  }
  recordJobDelayed(queueName, count) {
    const metrics = this.getOrCreateQueueMetrics(queueName);
    metrics.jobsDelayed = count;
  }
  getQueueMetrics(queueName) {
    return this.queueMetrics.get(queueName);
  }
  getAllMetrics() {
    return this.queueMetrics;
  }
  resetQueueMetrics(queueName) {
    this.queueMetrics.delete(queueName);
    this.logger.log(`Metrics reset for queue: ${queueName}`);
  }
  resetAllMetrics() {
    this.queueMetrics.clear();
    this.logger.log("All metrics reset");
  }
  getPrometheusMetrics() {
    const lines = [];
    for (const [queueName, metrics] of this.queueMetrics) {
      const label = `queue="${queueName}"`;
      lines.push(`# TYPE ${QueueMetricsService.METRIC_NAMES.JOBS_CREATED} counter`);
      lines.push(`${QueueMetricsService.METRIC_NAMES.JOBS_CREATED}{${label}} ${metrics.jobsCreated}`);
      lines.push(`# TYPE ${QueueMetricsService.METRIC_NAMES.JOBS_COMPLETED} counter`);
      lines.push(`${QueueMetricsService.METRIC_NAMES.JOBS_COMPLETED}{${label}} ${metrics.jobsCompleted}`);
      lines.push(`# TYPE ${QueueMetricsService.METRIC_NAMES.JOBS_FAILED} counter`);
      lines.push(`${QueueMetricsService.METRIC_NAMES.JOBS_FAILED}{${label}} ${metrics.jobsFailed}`);
      lines.push(`# TYPE ${QueueMetricsService.METRIC_NAMES.JOBS_ACTIVE} gauge`);
      lines.push(`${QueueMetricsService.METRIC_NAMES.JOBS_ACTIVE}{${label}} ${metrics.jobsActive}`);
      lines.push(`# TYPE ${QueueMetricsService.METRIC_NAMES.JOBS_WAITING} gauge`);
      lines.push(`${QueueMetricsService.METRIC_NAMES.JOBS_WAITING}{${label}} ${metrics.jobsWaiting}`);
    }
    return lines.join("\n");
  }
  getOrCreateQueueMetrics(queueName) {
    let metrics = this.queueMetrics.get(queueName);
    if (!metrics) {
      metrics = {
        jobsCreated: 0,
        jobsCompleted: 0,
        jobsFailed: 0,
        jobsActive: 0,
        jobsWaiting: 0,
        jobsDelayed: 0,
        averageDuration: 0,
        averageWaitTime: 0
      };
      this.queueMetrics.set(queueName, metrics);
    }
    return metrics;
  }
};
QueueMetricsService.METRIC_NAMES = {
  JOBS_CREATED: "smart_queue_jobs_created_total",
  JOBS_COMPLETED: "smart_queue_jobs_completed_total",
  JOBS_FAILED: "smart_queue_jobs_failed_total",
  JOBS_ACTIVE: "smart_queue_jobs_active",
  JOBS_WAITING: "smart_queue_jobs_waiting",
  JOBS_DELAYED: "smart_queue_jobs_delayed"
};
QueueMetricsService = __decorateClass([
  Injectable()
], QueueMetricsService);
var QueueHealthService = class {
  constructor(registry) {
    this.registry = registry;
    this.logger = new Logger(QueueHealthService.name);
    this.healthChecks = /* @__PURE__ */ new Map();
    this.healthCheckInterval = null;
    this.checkIntervalMs = 3e4;
  }
  async checkQueueHealth(queueName) {
    const connectionStatus = await this.checkRedisConnection(queueName);
    const jobCounts = await this.getJobCounts(queueName);
    const isHealthy = connectionStatus === "connected" && jobCounts.failed < 1e3;
    const healthCheck = {
      isHealthy,
      queueName,
      connectionStatus,
      lastCheck: /* @__PURE__ */ new Date(),
      jobCounts
    };
    if (!isHealthy && jobCounts.failed >= 1e3) {
      healthCheck.error = `High failure count: ${jobCounts.failed}`;
    }
    this.healthChecks.set(queueName, healthCheck);
    this.logger.debug(
      `Health check for ${queueName}: ${isHealthy ? "healthy" : "unhealthy"}`
    );
    return healthCheck;
  }
  async checkAllQueuesHealth() {
    const results = /* @__PURE__ */ new Map();
    const queueNames = this.registry.getRegisteredQueueNames();
    for (const queueName of queueNames) {
      const health = await this.checkQueueHealth(queueName);
      results.set(queueName, health);
    }
    return results;
  }
  async isHealthy(queueName) {
    const health = await this.checkQueueHealth(queueName);
    return health.isHealthy;
  }
  getLastHealthCheck(queueName) {
    return this.healthChecks.get(queueName);
  }
  startHealthCheckMonitor(intervalMs) {
    if (this.healthCheckInterval) {
      this.logger.warn("Health check monitor already running");
      return;
    }
    const interval = intervalMs || this.checkIntervalMs;
    this.healthCheckInterval = setInterval(async () => {
      try {
        await this.checkAllQueuesHealth();
      } catch (error) {
        this.logger.error("Health check monitor error", error);
      }
    }, interval);
    this.logger.log(`Health check monitor started (interval: ${interval}ms)`);
  }
  stopHealthCheckMonitor() {
    if (this.healthCheckInterval) {
      clearInterval(this.healthCheckInterval);
      this.healthCheckInterval = null;
      this.logger.log("Health check monitor stopped");
    }
  }
  async checkRedisConnection(queueName) {
    try {
      const queue = this.registry.getQueue(queueName);
      if (!queue) {
        return "disconnected";
      }
      const queueClient = queue.client;
      if (!queueClient) {
        return "disconnected";
      }
      const client = await queueClient;
      await client.ping();
      return "connected";
    } catch (error) {
      this.logger.error(`Redis connection error for ${queueName}:`, error);
      return "error";
    }
  }
  async getJobCounts(queueName) {
    try {
      const queue = this.registry.getQueue(queueName);
      if (!queue) {
        return {
          waiting: 0,
          active: 0,
          completed: 0,
          failed: 0,
          delayed: 0,
          paused: 0
        };
      }
      const counts = await queue.getJobCounts(
        "waiting",
        "active",
        "completed",
        "failed",
        "delayed",
        "paused"
      );
      return {
        waiting: counts.waiting || 0,
        active: counts.active || 0,
        completed: counts.completed || 0,
        failed: counts.failed || 0,
        delayed: counts.delayed || 0,
        paused: counts.paused || 0
      };
    } catch (error) {
      this.logger.error(`Error getting job counts for ${queueName}:`, error);
      return {
        waiting: 0,
        active: 0,
        completed: 0,
        failed: 0,
        delayed: 0,
        paused: 0
      };
    }
  }
  async onModuleDestroy() {
    this.stopHealthCheckMonitor();
  }
};
QueueHealthService = __decorateClass([
  Injectable()
], QueueHealthService);
var ProcessorScannerService = class {
  constructor(queueService, metadataScanner) {
    this.queueService = queueService;
    this.metadataScanner = metadataScanner;
    this.logger = new Logger(ProcessorScannerService.name);
    this.scannedQueues = /* @__PURE__ */ new Set();
  }
  async onModuleInit() {
    this.logger.log("ProcessorScannerService initialized");
  }
  scanAndRegister(modules) {
    for (const module of modules) {
      this.scanModule(module);
    }
    if (this.scannedQueues.size > 0) {
      this.logger.log(
        `Auto-registered workers for queues: ${Array.from(this.scannedQueues).join(", ")}`
      );
    }
  }
  scanModule(module) {
    const providers = module.providers;
    for (const [token, wrapper] of providers) {
      if (this.isProcessorClass(wrapper)) {
        const instance = wrapper.instance;
        this.registerProcessorClass(instance);
      }
    }
  }
  isProcessorClass(wrapper) {
    const instance = wrapper.instance;
    if (typeof instance !== "object" || !instance.constructor) {
      return false;
    }
    const metadata = getProcessorMetadata(instance.constructor);
    return !!metadata;
  }
  registerProcessorClass(instance) {
    const queueMeta = getProcessorMetadata(instance.constructor);
    const metadata = queueMeta;
    if (!metadata?.queue) {
      return;
    }
    this.scannedQueues.add(metadata.queue);
    const prototype = Object.getPrototypeOf(instance);
    const methodNames = this.metadataScanner.getAllMethodNames(prototype);
    for (const methodName of methodNames) {
      const handlerMeta = getQueueHandlerMetadata(instance, methodName);
      if (handlerMeta) {
        const handler = instance[methodName].bind(instance);
        const queueName = handlerMeta.options?.queue || metadata.queue;
        const concurrency = handlerMeta.options?.concurrency || metadata.concurrency || 1;
        this.queueService.worker(
          queueName,
          handler,
          {
            queue: queueName,
            concurrency,
            retryStrategy: handlerMeta.options?.retryStrategy
          }
        );
        this.logger.log(
          `Registered handler '${methodName}' for queue '${queueName}' (concurrency: ${concurrency})`
        );
      }
    }
    if (methodNames.length === 0) {
      this.logger.warn(
        `No handlers found in processor class for queue '${metadata.queue}'`
      );
    }
  }
};
ProcessorScannerService = __decorateClass([
  Injectable()
], ProcessorScannerService);
var SmartQueueModule = class {
  constructor() {
    this.logger = new Logger(SmartQueueModule.name);
    this.logger.log("SmartQueueModule initializing...");
  }
  static forRoot(options) {
    const providers = this.createProviders(options);
    return {
      module: SmartQueueModule,
      providers,
      exports: [
        QueueRegistry,
        BullMQAdapter,
        QueueService,
        QueueMetricsService,
        QueueHealthService
      ]
    };
  }
  static forRootAsync(options) {
    const providers = this.createAsyncProviders(options);
    return {
      module: SmartQueueModule,
      imports: options.imports || [],
      providers,
      exports: [
        QueueRegistry,
        BullMQAdapter,
        QueueService,
        QueueMetricsService,
        QueueHealthService
      ]
    };
  }
  static forFeature(queueNames) {
    const providers = [];
    for (const queueName of queueNames) {
      providers.push({
        provide: `QUEUE_${queueName.toUpperCase()}_SERVICE`,
        useFactory: (adapter) => {
          adapter.registerQueue(queueName);
          return adapter;
        },
        inject: [BullMQAdapter]
      });
    }
    return {
      module: SmartQueueModule,
      providers,
      imports: []
    };
  }
  static createProviders(options) {
    const metricsService = new QueueMetricsService();
    return [
      {
        provide: SMART_QUEUE_MODULE_OPTIONS,
        useValue: options || {}
      },
      {
        provide: QueueMetricsService,
        useValue: metricsService
      },
      {
        provide: QueueRegistry,
        useFactory: (moduleOptions) => {
          return new QueueRegistry(moduleOptions, metricsService);
        },
        inject: [SMART_QUEUE_MODULE_OPTIONS]
      },
      {
        provide: BullMQAdapter,
        useFactory: (registry, metrics) => {
          if (!registry) {
            throw new Error("QueueRegistry is not available. Ensure SmartQueueModule.forRoot() is called before using queues.");
          }
          return new BullMQAdapter(registry, metrics);
        },
        inject: [QueueRegistry, QueueMetricsService]
      },
      QueueHealthService,
      ProcessorScannerService,
      {
        provide: QueueService,
        useFactory: (adapter, healthService, metrics, moduleOptions) => {
          return new QueueService(adapter, healthService, metrics, moduleOptions);
        },
        inject: [BullMQAdapter, QueueHealthService, QueueMetricsService, SMART_QUEUE_MODULE_OPTIONS]
      }
    ];
  }
  static createAsyncProviders(options) {
    return [
      {
        provide: SMART_QUEUE_MODULE_OPTIONS,
        useFactory: async (factory) => {
          return factory.createSmartQueueOptions();
        },
        inject: [options.useClass]
      },
      {
        provide: options.useClass,
        useClass: options.useClass
      },
      {
        provide: QueueMetricsService,
        useValue: new QueueMetricsService()
      },
      {
        provide: QueueRegistry,
        useFactory: (moduleOptions, metricsService) => {
          return new QueueRegistry(moduleOptions, metricsService);
        },
        inject: [SMART_QUEUE_MODULE_OPTIONS, QueueMetricsService]
      },
      {
        provide: BullMQAdapter,
        useFactory: (registry, metrics) => {
          if (!registry) {
            throw new Error("QueueRegistry is not available. Ensure SmartQueueModule.forRoot() is called before using queues.");
          }
          return new BullMQAdapter(registry, metrics);
        },
        inject: [QueueRegistry, QueueMetricsService]
      },
      QueueHealthService,
      ProcessorScannerService,
      {
        provide: QueueService,
        useFactory: (adapter, healthService, metrics, moduleOptions) => {
          return new QueueService(adapter, healthService, metrics, moduleOptions);
        },
        inject: [BullMQAdapter, QueueHealthService, QueueMetricsService, SMART_QUEUE_MODULE_OPTIONS]
      }
    ];
  }
  configure(consumer) {
  }
  async onModuleDestroy() {
    this.logger.log("SmartQueueModule destroying...");
  }
};
SmartQueueModule = __decorateClass([
  Global(),
  Module({})
], SmartQueueModule);
var BullBoardModule = class {
  static forRoot(options) {
    return {
      module: BullBoardModule,
      providers: [
        {
          provide: "BULL_BOARD_OPTIONS",
          useValue: options || {}
        },
        {
          provide: BullBoardModule,
          useFactory: (registry) => {
            return new BullBoardModule(registry);
          },
          inject: [QueueRegistry]
        }
      ],
      imports: [],
      exports: [BullBoardModule]
    };
  }
  constructor(queueRegistry) {
    this.queueRegistry = queueRegistry;
  }
  async onModuleInit() {
    if (!this.queueRegistry) {
      throw new Error(
        "BullBoardModule requires QueueRegistry. Ensure SmartQueueModule.forRoot() is called before BullBoardModule.forRoot()."
      );
    }
  }
  static getQueues(queueRegistry) {
    const queues = [];
    if (!queueRegistry) {
      return queues;
    }
    const registeredQueueNames = queueRegistry.getRegisteredQueueNames();
    for (const name of registeredQueueNames) {
      const queue = queueRegistry.getQueue(name);
      if (queue) {
        queues.push(queue);
      }
    }
    return queues;
  }
};
BullBoardModule = __decorateClass([
  Module({}),
  __decorateParam(0, Optional()),
  __decorateParam(0, Inject(QueueRegistry))
], BullBoardModule);

export { BullBoardModule, BullMQAdapter, PROCESSOR_METADATA_KEY, Processor, ProcessorScannerService, QUEUE_HANDLER_METADATA_KEY, QUEUE_HEALTH_INDICATOR, QUEUE_METADATA_KEY, QUEUE_REGISTRY_TOKEN, QUEUE_SERVICE_TOKEN, Queue, QueueHandler, QueueHealthService, QueueMetricsService, QueueRegistry, QueueService, SMART_QUEUE_MODULE_OPTIONS, SmartQueueModule, getProcessorMetadata, getQueueHandlerMetadata, getQueueMetadata };
//# sourceMappingURL=index.mjs.map
//# sourceMappingURL=index.mjs.map