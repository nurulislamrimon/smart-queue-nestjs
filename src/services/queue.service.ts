import { Injectable, Inject, Logger } from '@nestjs/common';
import { Job } from 'bullmq';
import { BullMQAdapter } from '../adapters/bullmq.adapter';
import {
  QueueAddOptions,
  QueueDelayOptions,
  QueueRepeatOptions,
  QueueMetrics,
  JobEventHandlers,
  RetryStrategyOptions,
  ProcessorOptions,
  SmartQueueModuleOptions,
  SMART_QUEUE_MODULE_OPTIONS,
} from '../interfaces/queue-options.interface';

@Injectable()
export class QueueService {
  private readonly logger = new Logger(QueueService.name);
  private readonly defaultQueueOptions: Partial<SmartQueueModuleOptions>;

  constructor(
    private readonly adapter: BullMQAdapter,
    @Inject(SMART_QUEUE_MODULE_OPTIONS) private readonly moduleOptions?: SmartQueueModuleOptions,
  ) {
    this.defaultQueueOptions = moduleOptions || {};
  }

  async add<T = unknown>(
    queueName: string,
    name: string,
    data: T,
    options?: QueueAddOptions<T>,
  ): Promise<Job> {
    const job = await this.adapter.addJob<T>(queueName, name, data, options as QueueAddOptions<T>);
    this.logger.log(`Job added to queue '${queueName}': ${name}`);
    return job;
  }

  async delay<T = unknown>(
    queueName: string,
    name: string,
    data: T,
    delay: number,
    options?: Omit<QueueDelayOptions<T>, 'delay'>,
  ): Promise<Job<T>> {
    return this.adapter.addDelayedJob<T>(queueName, name, data, delay, options);
  }

  async repeat<T = unknown>(
    queueName: string,
    name: string,
    data: T,
    cron: string,
    options?: QueueRepeatOptions<T>,
  ): Promise<Job<T>> {
    return this.adapter.addRepeatableJob<T>(queueName, name, data, cron, options);
  }

  async remove(queueName: string, jobId: string): Promise<void> {
    await this.adapter.removeJob(queueName, jobId);
  }

  async getJob(queueName: string, jobId: string): Promise<Job | null> {
    return this.adapter.getJob(queueName, jobId);
  }

  async getMetrics(queueName: string): Promise<QueueMetrics> {
    return this.adapter.getMetrics(queueName);
  }

  async pause(queueName: string): Promise<void> {
    await this.adapter.pauseQueue(queueName);
  }

  async resume(queueName: string): Promise<void> {
    await this.adapter.resumeQueue(queueName);
  }

  async drain(queueName: string): Promise<void> {
    await this.adapter.drainQueue(queueName);
  }

  async clean(
    queueName: string,
    grace: number = 5000,
    status?: 'completed' | 'failed',
  ): Promise<string[]> {
    return this.adapter.cleanQueue(queueName, grace, status);
  }

  async removeRepeatable(queueName: string, jobKey: string): Promise<boolean> {
    return this.adapter.removeRepeatableJob(queueName, jobKey);
  }

  async getRepeatableJobs(queueName: string): Promise<Job[]> {
    return this.adapter.getRepeatableJobs(queueName);
  }

  worker<T = unknown>(
    queueName: string,
    processor: (job: { id: string; data: T }) => Promise<unknown>,
    options?: ProcessorOptions & { retryStrategy?: RetryStrategyOptions },
  ): void {
    this.adapter.createWorker<T>(queueName, processor, options);
  }

  on<T = unknown>(
    queueName: string,
    event: 'completed' | 'failed' | 'progress' | 'waiting' | 'stalled',
    handler: (...args: unknown[]) => void,
  ): void {
    const handlers: JobEventHandlers = {};
    
    switch (event) {
      case 'completed':
        handlers.completed = handler as (jobId: string, result: unknown) => void;
        break;
      case 'failed':
        handlers.failed = handler as (jobId: string, error: Error) => void;
        break;
      case 'progress':
        handlers.progress = handler as (jobId: string, progress: number | object) => void;
        break;
      case 'waiting':
        handlers.waiting = handler as (jobId: string) => void;
        break;
      case 'stalled':
        handlers.stalled = handler as (jobId: string) => void;
        break;
    }
    
    this.adapter.registerEventListeners(queueName, handlers);
  }

  async close(queueName: string): Promise<void> {
    await this.adapter.closeQueue(queueName);
  }
}
