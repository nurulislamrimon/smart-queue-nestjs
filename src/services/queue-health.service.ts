import { Injectable, Logger, OnModuleDestroy } from '@nestjs/common';
import { QueueRegistry } from '../core/queue-registry';
import { QueueHealthCheck, QueueMetrics } from '../interfaces/queue-options.interface';

@Injectable()
export class QueueHealthService implements OnModuleDestroy {
  private readonly logger = new Logger(QueueHealthService.name);
  private readonly healthChecks: Map<string, QueueHealthCheck> = new Map();
  private healthCheckInterval: NodeJS.Timeout | null = null;
  private readonly checkIntervalMs = 30000;

  constructor(private readonly registry: QueueRegistry) {}

  async checkQueueHealth(queueName: string): Promise<QueueHealthCheck> {
    const connectionStatus = await this.checkRedisConnection(queueName);
    const jobCounts = await this.getJobCounts(queueName);

    const isHealthy =
      connectionStatus === 'connected' && jobCounts.failed < 1000;

    const healthCheck: QueueHealthCheck = {
      isHealthy,
      queueName,
      connectionStatus,
      lastCheck: new Date(),
      jobCounts,
    };

    if (!isHealthy && jobCounts.failed >= 1000) {
      healthCheck.error = `High failure count: ${jobCounts.failed}`;
    }

    this.healthChecks.set(queueName, healthCheck);
    this.logger.debug(
      `Health check for ${queueName}: ${isHealthy ? 'healthy' : 'unhealthy'}`,
    );

    return healthCheck;
  }

  async checkAllQueuesHealth(): Promise<Map<string, QueueHealthCheck>> {
    const results = new Map<string, QueueHealthCheck>();
    const queueNames = this.registry.getRegisteredQueueNames();

    for (const queueName of queueNames) {
      const health = await this.checkQueueHealth(queueName);
      results.set(queueName, health);
    }

    return results;
  }

  async isHealthy(queueName: string): Promise<boolean> {
    const health = await this.checkQueueHealth(queueName);
    return health.isHealthy;
  }

  getLastHealthCheck(queueName: string): QueueHealthCheck | undefined {
    return this.healthChecks.get(queueName);
  }

  startHealthCheckMonitor(intervalMs?: number): void {
    if (this.healthCheckInterval) {
      this.logger.warn('Health check monitor already running');
      return;
    }

    const interval = intervalMs || this.checkIntervalMs;
    this.healthCheckInterval = setInterval(async () => {
      try {
        await this.checkAllQueuesHealth();
      } catch (error) {
        this.logger.error('Health check monitor error', error);
      }
    }, interval);

    this.logger.log(`Health check monitor started (interval: ${interval}ms)`);
  }

  stopHealthCheckMonitor(): void {
    if (this.healthCheckInterval) {
      clearInterval(this.healthCheckInterval);
      this.healthCheckInterval = null;
      this.logger.log('Health check monitor stopped');
    }
  }

  private async checkRedisConnection(
    queueName: string,
  ): Promise<'connected' | 'disconnected' | 'error'> {
    try {
      const queue = this.registry.getQueue(queueName);
      if (!queue) {
        return 'disconnected';
      }

      const queueClient = (queue as any).client;
      if (!queueClient) {
        return 'disconnected';
      }

      const client = await queueClient;
      await client.ping();
      return 'connected';
    } catch (error) {
      this.logger.error(`Redis connection error for ${queueName}:`, error);
      return 'error';
    }
  }

  private async getJobCounts(queueName: string): Promise<QueueMetrics> {
    try {
      const queue = this.registry.getQueue(queueName);
      if (!queue) {
        return {
          waiting: 0,
          active: 0,
          completed: 0,
          failed: 0,
          delayed: 0,
          paused: 0,
        };
      }

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
    } catch (error) {
      this.logger.error(`Error getting job counts for ${queueName}:`, error);
      return {
        waiting: 0,
        active: 0,
        completed: 0,
        failed: 0,
        delayed: 0,
        paused: 0,
      };
    }
  }

  async onModuleDestroy(): Promise<void> {
    this.stopHealthCheckMonitor();
  }
}