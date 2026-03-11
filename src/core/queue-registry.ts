import {
  Injectable,
  OnModuleDestroy,
  Logger,
} from '@nestjs/common';
import { Queue, Worker, QueueEvents, ConnectionOptions } from 'bullmq';
import {
  SmartQueueModuleOptions,
  ProcessorOptions,
  JobEventHandlers,
  RetryStrategyOptions,
} from '../interfaces/queue-options.interface';

interface RegisteredQueue {
  queue: Queue;
  queueEvents: QueueEvents;
}

interface RegisteredWorker {
  worker: Worker;
  queueName: string;
}

@Injectable()
export class QueueRegistry implements OnModuleDestroy {
  private readonly logger = new Logger(QueueRegistry.name);
  private readonly queues: Map<string, RegisteredQueue> = new Map();
  private readonly workers: Map<string, RegisteredWorker> = new Map();
  private readonly defaultOptions: Partial<SmartQueueModuleOptions>;

  constructor(defaultOptions?: SmartQueueModuleOptions) {
    this.defaultOptions = defaultOptions || {};
  }

  private getConnectionOptions(): ConnectionOptions {
    const connection = this.defaultOptions.connection || {};
    return {
      host: connection.host || 'localhost',
      port: connection.port || 6379,
      db: connection.db || 0,
      password: connection.password,
    };
  }

  getOrCreateQueue(name: string, options?: Partial<SmartQueueModuleOptions>): Queue {
    const queueName = this.getPrefixedQueueName(name);
    
    if (this.queues.has(queueName)) {
      return this.queues.get(queueName)!.queue;
    }

    const queueOptions: ConstructorParameters<typeof Queue>[1] = {
      connection: this.getConnectionOptions(),
      defaultJobOptions: {
        ...this.defaultOptions.defaultJobOptions,
        ...options?.defaultJobOptions,
      },
    };

    const queue = new Queue(queueName, queueOptions);
    const queueEvents = new QueueEvents(queueName, {
      connection: this.getConnectionOptions(),
    });

    this.queues.set(queueName, { queue, queueEvents });
    this.logger.log(`Queue created: ${queueName}`);

    return queue;
  }

  getQueue(name: string): Queue | undefined {
    const queueName = this.getPrefixedQueueName(name);
    return this.queues.get(queueName)?.queue;
  }

  getQueueEvents(name: string): QueueEvents | undefined {
    const queueName = this.getPrefixedQueueName(name);
    return this.queues.get(queueName)?.queueEvents;
  }

  createWorker<T = unknown>(
    name: string,
    processor: (job: { id: string; data: T }) => Promise<unknown>,
    options?: ProcessorOptions & { retryStrategy?: RetryStrategyOptions },
  ): Worker {
    const queueName = this.getPrefixedQueueName(name);
    const workerKey = `${queueName}-${options?.concurrency || 1}`;

    if (this.workers.has(workerKey)) {
      return this.workers.get(workerKey)!.worker;
    }

    const workerOptions: ConstructorParameters<typeof Worker>[2] = {
      connection: this.getConnectionOptions(),
      concurrency: options?.concurrency || 1,
      limiter: options?.limiter,
      lockDuration: options?.lockDuration,
      lockRenewTime: options?.lockRenewTime,
      maxStalledCount: options?.maxStalledCount,
      stalledInterval: options?.stalledInterval,
    };

    const worker = new Worker<T>(queueName, async (job) => {
      try {
        const jobId = job.id || 'unknown';
        const result = await processor({ id: jobId, data: job.data });
        return result;
      } catch (error) {
        const retryStrategy = options?.retryStrategy;
        
        if (retryStrategy) {
          const shouldRetry = job.attemptsMade < retryStrategy.attempts;
          
          if (shouldRetry) {
            let delay: number | undefined;
            
            if (typeof retryStrategy.backoff === 'number') {
              delay = retryStrategy.backoff;
            } else if (retryStrategy.backoff?.type === 'exponential') {
              delay = Math.pow(2, job.attemptsMade) * (retryStrategy.backoff.delay || 1000);
            } else if (retryStrategy.backoff?.type === 'fixed') {
              delay = retryStrategy.backoff.delay || 1000;
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

    worker.on('completed', (job) => {
      this.logger.debug(`Job ${job.id} completed in queue ${queueName}`);
    });

    worker.on('failed', (job, error) => {
      this.logger.error(`Job ${job?.id} failed in queue ${queueName}: ${error.message}`);
    });

    worker.on('stalled', (jobId) => {
      this.logger.warn(`Job ${jobId} stalled in queue ${queueName}`);
    });

    this.workers.set(workerKey, { worker, queueName: queueName });
    this.logger.log(`Worker created for queue: ${queueName}`);

    return worker;
  }

  registerEventListeners(
    queueName: string,
    handlers: JobEventHandlers,
  ): void {
    const queueEvents = this.getQueueEvents(queueName);
    if (!queueEvents) {
      this.logger.warn(`Queue events not found for queue: ${queueName}`);
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
      await registered.queueEvents.close();
      await registered.queue.close();
      this.queues.delete(queueName);
      this.logger.log(`Queue closed: ${queueName}`);
    }
  }

  async closeWorker(name: string): Promise<void> {
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

  async closeAll(): Promise<void> {
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

    this.logger.log('All queues and workers closed');
  }

  async onModuleDestroy(): Promise<void> {
    await this.closeAll();
  }

  private getPrefixedQueueName(name: string): string {
    const prefix = this.defaultOptions.connection?.keyPrefix || 'smart-queue';
    return `${prefix}:${name}`;
  }
}
