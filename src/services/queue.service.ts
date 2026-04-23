import { Injectable, Inject, Logger, OnModuleDestroy } from '@nestjs/common';
import { Job } from 'bullmq';
import { BullMQAdapter } from '../adapters/bullmq.adapter';
import {
  QueueAddOptions,
  QueueDelayOptions,
  QueueRepeatOptions,
  QueueMetrics,
  JobEventHandlers,
  ProcessorOptions,
  RetryStrategyOptions,
  DeadLetterQueueOptions,
  JobHooks,
  SmartQueueModuleOptions,
  SMART_QUEUE_MODULE_OPTIONS,
  QueueHealthCheck,
} from '../interfaces/queue-options.interface';
import { QueueHealthService } from './queue-health.service';
import { QueueMetricsService } from './queue-metrics.service';

export interface QueueJob<T = unknown> {
  id: string;
  name: string;
  data: T;
  progress: number | object;
}

@Injectable()
export class QueueService implements OnModuleDestroy {
  private readonly logger = new Logger(QueueService.name);
  private readonly defaultQueueOptions: Partial<SmartQueueModuleOptions>;

  constructor(
    private readonly adapter: BullMQAdapter,
    private readonly healthService: QueueHealthService,
    private readonly metricsService: QueueMetricsService,
    @Inject(SMART_QUEUE_MODULE_OPTIONS) moduleOptions?: SmartQueueModuleOptions,
  ) {
    this.defaultQueueOptions = moduleOptions || {};
  }

  registerQueue(name: string, options?: Partial<SmartQueueModuleOptions>): void {
    this.adapter.registerQueue(name, options);
  }

  registerDeadLetterQueue(queueName: string, options: DeadLetterQueueOptions): void {
    this.adapter.registerDeadLetterQueue(queueName, options);
  }

  registerJobHooks(queueName: string, hooks: JobHooks): void {
    this.adapter.registerJobHooks(queueName, hooks);
  }

  async add<T = unknown>(
    queueName: string,
    name: string,
    data: T,
    options?: QueueAddOptions<T>,
  ): Promise<Job> {
    return this.adapter.addJob<T>(queueName, name, data, options as QueueAddOptions<T>);
  }

  async addWithIdempotencyKey<T = unknown>(
    queueName: string,
    name: string,
    data: T,
    idempotencyKey: string,
    options?: QueueAddOptions<T>,
  ): Promise<Job> {
    const existingJob = await this.adapter.getJob(queueName, idempotencyKey);

    if (existingJob) {
      this.logger.warn(
        `Job with idempotency key '${idempotencyKey}' already exists. Returning existing job.`,
      );
      return existingJob;
    }

    return this.adapter.addJob<T>(queueName, name, data, {
      ...options,
      jobId: idempotencyKey,
    } as QueueAddOptions<T>);
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

  async getQueueMetrics(queueName: string): Promise<QueueMetrics> {
    return this.adapter.getQueueMetrics(queueName);
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

  async checkHealth(queueName: string): Promise<QueueHealthCheck> {
    return this.healthService.checkQueueHealth(queueName);
  }

  async checkAllHealth(): Promise<Map<string, QueueHealthCheck>> {
    return this.healthService.checkAllQueuesHealth();
  }

  async isHealthy(queueName: string): Promise<boolean> {
    return this.healthService.isHealthy(queueName);
  }

  getMetrics(): string {
    return this.metricsService.getPrometheusMetrics();
  }

  async close(queueName: string): Promise<void> {
    await this.adapter.closeQueue(queueName);
  }

  async onModuleDestroy(): Promise<void> {
    await this.adapter.onModuleDestroy();
  }
}