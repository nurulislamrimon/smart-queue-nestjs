import { Injectable, Logger } from '@nestjs/common';
import { Queue, Job, RepeatOptions } from 'bullmq';
import { QueueRegistry } from '../core/queue-registry';
import {
  QueueAddOptions,
  QueueDelayOptions,
  QueueRepeatOptions,
  QueueMetrics,
  JobEventHandlers,
  RetryStrategyOptions,
  ProcessorOptions,
} from '../interfaces/queue-options.interface';

@Injectable()
export class BullMQAdapter {
  private readonly logger = new Logger(BullMQAdapter.name);

  constructor(private readonly registry: QueueRegistry) {}

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

    this.logger.debug(`Job added to queue ${queueName}: ${job.id}`);
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

    this.logger.debug(`Repeatable job added to queue ${queueName} with cron: ${cron}`);
    return job;
  }

  async removeJob(queueName: string, jobId: string): Promise<void> {
    const queue = this.getQueue(queueName);
    const job = await queue.getJob(jobId);
    
    if (job) {
      await job.remove();
      this.logger.debug(`Job ${jobId} removed from queue ${queueName}`);
    }
  }

  async getJob(queueName: string, jobId: string): Promise<Job | null> {
    const queue = this.getQueue(queueName);
    const job = await queue.getJob(jobId);
    return job ?? null;
  }

  async getMetrics(queueName: string): Promise<QueueMetrics> {
    const queue = this.getQueue(queueName);
    
    const counts = await queue.getJobCounts('waiting', 'active', 'completed', 'failed', 'delayed', 'paused');
    
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
    this.logger.log(`Queue ${queueName} paused`);
  }

  async resumeQueue(queueName: string): Promise<void> {
    const queue = this.getQueue(queueName);
    await queue.resume();
    this.logger.log(`Queue ${queueName} resumed`);
  }

  async drainQueue(queueName: string): Promise<void> {
    const queue = this.getQueue(queueName);
    await queue.drain();
    this.logger.log(`Queue ${queueName} drained`);
  }

  async cleanQueue(queueName: string, grace: number, status?: 'completed' | 'failed'): Promise<string[]> {
    const queue = this.getQueue(queueName);
    const jobs = await queue.clean(grace, 100, status);
    this.logger.debug(`Cleaned ${jobs.length} jobs from queue ${queueName}`);
    return jobs;
  }

  createWorker<T = unknown>(
    queueName: string,
    processor: (job: { id: string; data: T }) => Promise<unknown>,
    options?: ProcessorOptions & { retryStrategy?: RetryStrategyOptions },
  ): void {
    this.registry.createWorker(queueName, processor, options);
  }

  registerEventListeners(
    queueName: string,
    handlers: JobEventHandlers,
  ): void {
    this.registry.registerEventListeners(queueName, handlers);
  }

  async removeRepeatableJob(
    queueName: string,
    jobKey: string,
  ): Promise<boolean> {
    const queue = this.getQueue(queueName);
    const removed = await (queue as any).removeRepeatable(jobKey);
    this.logger.debug(`Removed repeatable job ${jobKey} from queue ${queueName}`);
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
}
