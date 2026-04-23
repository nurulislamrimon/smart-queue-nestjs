import { Injectable, Logger, OnModuleDestroy } from '@nestjs/common';
import { Queue, Job, RepeatOptions } from 'bullmq';
import { QueueRegistry } from '../core/queue-registry';
import {
  QueueAddOptions,
  QueueDelayOptions,
  QueueRepeatOptions,
  QueueMetrics,
  JobEventHandlers,
  ProcessorOptions,
  DeadLetterQueueOptions,
  JobHooks,
} from '../interfaces/queue-options.interface';
import { QueueMetricsService } from '../services/queue-metrics.service';

@Injectable()
export class BullMQAdapter implements OnModuleDestroy {
  private readonly logger = new Logger(BullMQAdapter.name);

  constructor(
    private readonly registry: QueueRegistry,
    private readonly metricsService?: QueueMetricsService,
  ) {}

  registerQueue(name: string, options?: any): void {
    this.registry.registerQueue(name, options);
  }

  registerDeadLetterQueue(name: string, options: DeadLetterQueueOptions): void {
    this.registry.registerDeadLetterQueue(name, options);
  }

  registerJobHooks(name: string, hooks: JobHooks): void {
    this.registry.registerJobHooks(name, hooks);
  }

  getQueue(name: string): Queue {
    const queue = this.registry.getQueue(name);

    if (!queue) {
      return this.registry.getOrCreateQueue(name);
    }

    return queue;
  }

  async addJob<T = unknown>(
    queueName: string,
    name: string,
    data: T,
    options?: QueueAddOptions<T>,
  ): Promise<Job> {
    const queue = this.getQueue(queueName);

    const job = await queue.add(name, data, {
      jobId: options?.jobId,
      priority: options?.priority,
      delay: options?.delay,
      attempts: options?.attempts,
      backoff: options?.backoff as any,
      lifo: options?.lifo,
      timeout: options?.timeout,
      removeOnComplete: options?.removeOnComplete ?? true,
      removeOnFail: options?.removeOnFail ?? 100,
      repeatJobKey: options?.repeatJobKey,
    } as any);

    this.metricsService?.recordJobCreated(queueName);
    this.logger.log(
      `Job ${job.id} added to queue '${queueName}': ${name}`,
      'BullMQAdapter',
    );

    return job;
  }

  async addDelayedJob<T = unknown>(
    queueName: string,
    name: string,
    data: T,
    delay: number,
    options?: Omit<QueueDelayOptions<T>, 'delay'>,
  ): Promise<Job> {
    return this.addJob<T>(queueName, name, data, {
      ...options,
      delay,
    } as QueueAddOptions<T>);
  }

  async addRepeatableJob<T = unknown>(
    queueName: string,
    name: string,
    data: T,
    cron: string,
    options?: QueueRepeatOptions<T>,
  ): Promise<Job> {
    const queue = this.getQueue(queueName);

    const repeatOptions: RepeatOptions = {
      pattern: cron,
      tz: options?.tz,
      startDate: options?.startDate ? new Date(options.startDate) : undefined,
      endDate: options?.endDate ? new Date(options.endDate) : undefined,
      limit: options?.limit,
    };

    const job = await queue.add(name, data, {
      jobId: options?.jobId,
      repeat: repeatOptions,
    } as any);

    this.metricsService?.recordJobCreated(queueName);
    this.logger.debug(
      `Repeatable job added to queue ${queueName} with cron: ${cron}`,
      'BullMQAdapter',
    );

    return job;
  }

  async removeJob(queueName: string, jobId: string): Promise<void> {
    const queue = this.getQueue(queueName);
    const job = await queue.getJob(jobId);

    if (job) {
      await job.remove();
      this.logger.log(
        `Job ${jobId} removed from queue ${queueName}`,
        'BullMQAdapter',
      );
    }
  }

  async getJob(queueName: string, jobId: string): Promise<Job | null> {
    const queue = this.getQueue(queueName);
    const job = await queue.getJob(jobId);
    return job ?? null;
  }

  async getQueueMetrics(queueName: string): Promise<QueueMetrics> {
    const queue = this.getQueue(queueName);

    const counts = await queue.getJobCounts(
      'waiting',
      'active',
      'completed',
      'failed',
      'delayed',
      'paused',
    );

    return {
      waiting: counts.waiting || 0,
      active: counts.active || 0,
      completed: counts.completed || 0,
      failed: counts.failed || 0,
      delayed: counts.delayed || 0,
      paused: counts.paused || 0,
    };
  }

  async pauseQueue(queueName: string): Promise<void> {
    const queue = this.getQueue(queueName);
    await queue.pause();
    this.logger.log(`Queue ${queueName} paused`, 'BullMQAdapter');
  }

  async resumeQueue(queueName: string): Promise<void> {
    const queue = this.getQueue(queueName);
    await queue.resume();
    this.logger.log(`Queue ${queueName} resumed`, 'BullMQAdapter');
  }

  async drainQueue(queueName: string): Promise<void> {
    const queue = this.getQueue(queueName);
    await queue.drain();
    this.logger.log(`Queue ${queueName} drained`, 'BullMQAdapter');
  }

  async cleanQueue(
    queueName: string,
    grace: number,
    status?: 'completed' | 'failed',
  ): Promise<string[]> {
    const queue = this.getQueue(queueName);
    const jobs = await queue.clean(grace, 100, status);
    this.logger.log(
      `Cleaned ${jobs.length} jobs from queue ${queueName}`,
      'BullMQAdapter',
    );
    return jobs;
  }

  createWorker<T = unknown>(
    queueName: string,
    processor: (job: { id: string; data: T }) => Promise<unknown>,
    options?: ProcessorOptions & { retryStrategy?: any },
  ): void {
    this.registry.createWorker<T>(queueName, processor, options);
  }

  registerEventListeners(queueName: string, handlers: JobEventHandlers): void {
    this.registry.registerEventListeners(queueName, handlers);
  }

  async removeRepeatableJob(queueName: string, jobKey: string): Promise<boolean> {
    const queue = this.getQueue(queueName);
    const removed = await (queue as any).removeRepeatable(jobKey);
    this.logger.debug(
      `Removed repeatable job ${jobKey} from queue ${queueName}`,
      'BullMQAdapter',
    );
    return removed;
  }

  async getRepeatableJobs(queueName: string): Promise<any[]> {
    const queue = this.getQueue(queueName);
    return queue.getRepeatableJobs();
  }

  async closeQueue(queueName: string): Promise<void> {
    await this.registry.closeQueue(queueName);
  }

  async closeWorker(queueName: string): Promise<void> {
    await this.registry.closeWorker(queueName);
  }

  getQueueNames(): string[] {
    return this.registry.getRegisteredQueueNames();
  }

  async onModuleDestroy(): Promise<void> {
    await this.registry.closeAll();
  }
}