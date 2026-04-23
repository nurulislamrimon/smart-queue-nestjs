import {
  Injectable,
  OnModuleDestroy,
  Logger,
  OnModuleInit,
} from '@nestjs/common';
import { Queue, Worker, QueueEvents, ConnectionOptions } from 'bullmq';
import {
  SmartQueueModuleOptions,
  ProcessorOptions,
  JobEventHandlers,
  RetryStrategyOptions,
  DeadLetterQueueOptions,
  JobHooks,
  QueueAddOptions,
} from '../interfaces/queue-options.interface';
import {
  SmartQueueConnectionOptions,
  BackoffStrategy,
} from '../interfaces/queue-options.interface';
import { QueueMetricsService } from '../services/queue-metrics.service';

interface RegisteredQueue {
  queue: Queue;
  queueEvents: QueueEvents;
  deadLetterQueue?: Queue;
}

interface RegisteredWorker {
  worker: Worker;
  queueName: string;
}

interface QueueConfig {
  name: string;
  options?: Partial<SmartQueueModuleOptions>;
  deadLetterOptions?: DeadLetterQueueOptions;
}

@Injectable()
export class QueueRegistry implements OnModuleDestroy, OnModuleInit {
  private readonly logger = new Logger(QueueRegistry.name);
  private readonly queues: Map<string, RegisteredQueue> = new Map();
  private readonly workers: Map<string, RegisteredWorker> = new Map();
  private readonly queueConfigs: Map<string, QueueConfig> = new Map();
  private readonly defaultOptions: Partial<SmartQueueModuleOptions>;
  private readonly jobHooks: Map<string, JobHooks> = new Map();
  private isInitialized = false;

  constructor(
    defaultOptions?: SmartQueueModuleOptions,
    private readonly metricsService?: QueueMetricsService,
  ) {
    this.defaultOptions = defaultOptions || {};
  }

  async onModuleInit(): Promise<void> {
    this.isInitialized = true;
    this.logger.log('QueueRegistry initialized');
  }

  private getConnectionOptions(): ConnectionOptions {
    const connection = this.defaultOptions.connection || {};
    const options: ConnectionOptions = {
      host: connection.host || 'localhost',
      port: connection.port || 6379,
      db: connection.db || 0,
      password: connection.password,
      family: connection.family,
      keyPrefix: connection.keyPrefix,
      connectTimeout: connection.connectTimeout,
      maxRetriesPerRequest: connection.maxRetriesPerRequest,
      enableOfflineQueue: connection.enableOfflineQueue,
    };

    if (connection.tls?.enabled) {
      options.tls = {
        ca: connection.tls.ca,
        cert: connection.tls.cert,
        key: connection.tls.key,
        rejectUnauthorized: connection.tls.rejectUnauthorized,
      };
    }

    return options;
  }

  registerQueue(name: string, options?: Partial<SmartQueueModuleOptions>): void {
    const queueName = this.getPrefixedQueueName(name);
    this.queueConfigs.set(queueName, { name, options });
  }

  registerDeadLetterQueue(
    mainQueueName: string,
    options: DeadLetterQueueOptions,
  ): void {
    const queueName = this.getPrefixedQueueName(mainQueueName);
    const config = this.queueConfigs.get(queueName);

    if (config) {
      config.deadLetterOptions = options;
    }
  }

  getOrCreateQueue(name: string): Queue {
    const queueName = this.getPrefixedQueueName(name);

    if (this.queues.has(queueName)) {
      return this.queues.get(queueName)!.queue;
    }

    const connectionOptions = this.getConnectionOptions();
    const config = this.queueConfigs.get(queueName);

    const queueOptions = {
      connection: connectionOptions,
      defaultJobOptions: {
        ...this.defaultOptions.defaultJobOptions,
        ...config?.options?.defaultJobOptions,
      },
    };

    const queue = new Queue(queueName, queueOptions);
    const queueEvents = new QueueEvents(queueName, {
      connection: connectionOptions,
    });

    let deadLetterQueue: Queue | undefined;

    if (config?.deadLetterOptions?.enabled) {
      const dlqName = `${queueName}:dlq`;
      deadLetterQueue = new Queue(dlqName, {
        connection: connectionOptions,
      });
    }

    this.queues.set(queueName, { queue, queueEvents, deadLetterQueue });
    this.logger.log(`Queue created: ${queueName}`, 'QueueRegistry');

    return queue;
  }

  getQueue(name: string): Queue | undefined {
    const queueName = this.getPrefixedQueueName(name);
    return this.queues.get(queueName)?.queue;
  }

  getDeadLetterQueue(name: string): Queue | undefined {
    const queueName = this.getPrefixedQueueName(name);
    return this.queues.get(queueName)?.deadLetterQueue;
  }

  getQueueEvents(name: string): QueueEvents | undefined {
    const queueName = this.getPrefixedQueueName(name);
    return this.queues.get(queueName)?.queueEvents;
  }

  getRegisteredQueueNames(): string[] {
    return Array.from(this.queues.keys());
  }

  registerJobHooks(queueName: string, hooks: JobHooks): void {
    const prefixedName = this.getPrefixedQueueName(queueName);
    this.jobHooks.set(prefixedName, hooks);
  }

  getJobHooks(queueName: string): JobHooks | undefined {
    const prefixedName = this.getPrefixedQueueName(queueName);
    return this.jobHooks.get(prefixedName);
  }

  createWorker<T = unknown>(
    name: string,
    processor: (job: { id: string; data: T }) => Promise<unknown>,
    options?: ProcessorOptions & { retryStrategy?: RetryStrategyOptions },
  ): Worker {
    const queueName = this.getPrefixedQueueName(name);
    const workerKey = `${queueName}-${options?.concurrency || 1}`;

    if (this.workers.has(workerKey)) {
      this.logger.warn(
        `Worker already exists for queue ${queueName}. Returning existing worker.`,
      );
      return this.workers.get(workerKey)!.worker;
    }

    const connectionOptions = this.getConnectionOptions();

    const workerOptions = {
      connection: connectionOptions,
      concurrency: options?.concurrency || 1,
      limiter: options?.limiter,
      lockDuration: options?.lockDuration,
      lockRenewTime: options?.lockRenewTime,
      maxStalledCount: options?.maxStalledCount,
      stalledInterval: options?.stalledInterval,
    };

    const config = this.queueConfigs.get(queueName);
    const retryStrategy = options?.retryStrategy;
    const dlqOptions = config?.deadLetterOptions;
    const hooks = this.jobHooks.get(queueName);

    const wrappedProcessor = async (job: any) => {
      const jobId = job.id || 'unknown';
      const startTime = Date.now();

      try {
        this.metricsService?.recordJobActive(queueName);
        this.logger.log(
          `Job ${jobId} started processing (attempt ${job.attemptsMade + 1})`,
          'QueueRegistry',
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
          'QueueRegistry',
        );

        return result;
      } catch (error: any) {
        const attempt = job.attemptsMade || 0;
        const maxAttempts = retryStrategy?.maxAttempts || 3;
        const canRetry = retryStrategy?.enabled !== false && attempt < maxAttempts;

        if (canRetry) {
          const delay = this.calculateBackoff(attempt, retryStrategy?.backoff);
          this.logger.warn(
            `Job ${jobId} failed, will retry after ${delay}ms (attempt ${attempt + 1}/${maxAttempts})`,
            'QueueRegistry',
          );

          if (delay > 0) {
            await job.updateProgress(-1);
            const err = new Error(
              `Job failed, will retry after ${delay}ms. Attempt ${attempt + 1}/${maxAttempts}`,
            );
            (err as any).retryDelay = delay;
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
          'QueueRegistry',
        );

        throw error;
      }
    };

    const worker = new Worker<T>(queueName, wrappedProcessor, workerOptions);

    worker.on('completed', (job) => {
      this.logger.debug(`Job ${job.id} completed`, 'QueueRegistry');
    });

    worker.on('failed', (job, error) => {
      this.logger.error(
        `Job ${job?.id} failed: ${error.message}`,
        error.stack,
        'QueueRegistry',
      );
    });

    worker.on('stalled', (jobId) => {
      this.logger.warn(`Job ${jobId} stalled`, 'QueueRegistry');
    });

    this.workers.set(workerKey, { worker, queueName: queueName });
    this.logger.log(
      `Worker created for queue: ${queueName} (concurrency: ${options?.concurrency || 1})`,
      'QueueRegistry',
    );

    return worker;
  }

  private calculateBackoff(
    attempt: number,
    backoff?: number | { type: 'exponential' | 'fixed'; delay: number },
  ): number {
    if (!backoff) {
      return 0;
    }

    if (typeof backoff === 'number') {
      return backoff;
    }

    if (backoff.type === 'exponential') {
      return Math.pow(2, attempt) * backoff.delay;
    }

    return backoff.delay;
  }

  private async moveToDeadLetterQueue(
    job: any,
    error: Error,
  ): Promise<void> {
    const queueName = this.getPrefixedQueueName(job.queueName);
    const registered = this.queues.get(queueName);

    if (!registered?.deadLetterQueue) {
      return;
    }

    const dlqJob = {
      data: job.data,
      opts: {
        jobId: `dlq-${job.id}`,
      },
    };

    await registered.deadLetterQueue.add(job.name, {
      ...dlqJob.data,
      __originalJobId__: job.id,
      __failureReason__: error.message,
      __failedAt__: new Date().toISOString(),
    });

    this.logger.warn(
      `Job ${job.id} moved to dead letter queue`,
      'QueueRegistry',
    );
  }

  registerEventListeners(queueName: string, handlers: JobEventHandlers): void {
    const prefixedName = this.getPrefixedQueueName(queueName);
    const queueEvents = this.getQueueEvents(prefixedName);

    if (!queueEvents) {
      this.logger.warn(
        `Queue events not found for queue: ${queueName}`,
        'QueueRegistry',
      );
      return;
    }

    if (handlers.completed) {
      queueEvents.on('completed', ({ jobId, returnvalue }) => {
        handlers.completed!(jobId, returnvalue);
      });
    }

    if (handlers.failed) {
      queueEvents.on('failed', ({ jobId, failedReason }) => {
        handlers.failed!(jobId, new Error(failedReason ?? 'Unknown error'));
      });
    }

    if (handlers.progress) {
      queueEvents.on('progress', (data: { jobId: string; data: any }) => {
        handlers.progress!(data.jobId, data.data as number | object);
      });
    }

    if (handlers.waiting) {
      queueEvents.on('waiting', ({ jobId }) => {
        handlers.waiting!(jobId);
      });
    }

    if (handlers.stalled) {
      queueEvents.on('stalled', ({ jobId }) => {
        handlers.stalled!(jobId);
      });
    }
  }

  async closeQueue(name: string): Promise<void> {
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
      this.logger.log(`Queue closed: ${queueName}`, 'QueueRegistry');
    }
  }

  async closeWorker(name: string): Promise<void> {
    const queueName = this.getPrefixedQueueName(name);

    for (const [key, workerEntry] of this.workers.entries()) {
      if (workerEntry.queueName === queueName) {
        await workerEntry.worker.close();
        this.workers.delete(key);
        this.logger.log(`Worker closed for queue: ${queueName}`, 'QueueRegistry');
        break;
      }
    }
  }

  async closeAll(): Promise<void> {
    this.logger.log('Closing all queues and workers...', 'QueueRegistry');

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
    this.logger.log('All queues and workers closed', 'QueueRegistry');
  }

  async onModuleDestroy(): Promise<void> {
    await this.closeAll();
    this.logger.log('QueueRegistry destroyed', 'QueueRegistry');
  }

  private getPrefixedQueueName(name: string): string {
    const prefix = this.defaultOptions.prefix || this.defaultOptions.connection?.keyPrefix || 'smart-queue';
    return `${prefix}:${name}`;
  }
}