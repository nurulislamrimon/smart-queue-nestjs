import { Injectable, Logger } from '@nestjs/common';

export interface QueueMetricsData {
  jobsCreated: number;
  jobsCompleted: number;
  jobsFailed: number;
  jobsActive: number;
  jobsWaiting: number;
  jobsDelayed: number;
  averageDuration: number;
  averageWaitTime: number;
}

export interface WorkerMetrics {
  jobsProcessed: number;
  jobsSucceeded: number;
  jobsFailed: number;
  durations: number[];
}

@Injectable()
export class QueueMetricsService {
  private readonly logger = new Logger(QueueMetricsService.name);
  private readonly queueMetrics: Map<string, QueueMetricsData> = new Map();
  private readonly workerMetrics: Map<string, WorkerMetrics> = new Map();

  private static readonly METRIC_NAMES = {
    JOBS_CREATED: 'smart_queue_jobs_created_total',
    JOBS_COMPLETED: 'smart_queue_jobs_completed_total',
    JOBS_FAILED: 'smart_queue_jobs_failed_total',
    JOBS_ACTIVE: 'smart_queue_jobs_active',
    JOBS_WAITING: 'smart_queue_jobs_waiting',
    JOBS_DELAYED: 'smart_queue_jobs_delayed',
  } as const;

  recordJobCreated(queueName: string): void {
    const metrics = this.getOrCreateQueueMetrics(queueName);
    metrics.jobsCreated++;
    this.logger.debug(
      `Job created in queue ${queueName}. Total: ${metrics.jobsCreated}`,
      'QueueMetrics',
    );
  }

  recordJobCompleted(queueName: string, durationMs: number): void {
    const metrics = this.getOrCreateQueueMetrics(queueName);
    metrics.jobsCompleted++;
    metrics.jobsActive = Math.max(0, metrics.jobsActive - 1);

    if (metrics.averageDuration === 0) {
      metrics.averageDuration = durationMs;
    } else {
      metrics.averageDuration =
        (metrics.averageDuration * (metrics.jobsCompleted - 1) + durationMs) /
        metrics.jobsCompleted;
    }

    this.logger.debug(
      `Job completed in queue ${queueName}. Avg duration: ${metrics.averageDuration.toFixed(2)}ms`,
      'QueueMetrics',
    );
  }

  recordJobFailed(queueName: string): void {
    const metrics = this.getOrCreateQueueMetrics(queueName);
    metrics.jobsFailed++;
    metrics.jobsActive = Math.max(0, metrics.jobsActive - 1);
    this.logger.debug(
      `Job failed in queue ${queueName}. Total failures: ${metrics.jobsFailed}`,
      'QueueMetrics',
    );
  }

  recordJobActive(queueName: string): void {
    const metrics = this.getOrCreateQueueMetrics(queueName);
    metrics.jobsActive++;
    this.logger.debug(
      `Job activated in queue ${queueName}. Active: ${metrics.jobsActive}`,
      'QueueMetrics',
    );
  }

  recordJobWaiting(queueName: string, count: number): void {
    const metrics = this.getOrCreateQueueMetrics(queueName);
    metrics.jobsWaiting = count;
  }

  recordJobDelayed(queueName: string, count: number): void {
    const metrics = this.getOrCreateQueueMetrics(queueName);
    metrics.jobsDelayed = count;
  }

  getQueueMetrics(queueName: string): QueueMetricsData | undefined {
    return this.queueMetrics.get(queueName);
  }

  getAllMetrics(): Map<string, QueueMetricsData> {
    return this.queueMetrics;
  }

  resetQueueMetrics(queueName: string): void {
    this.queueMetrics.delete(queueName);
    this.logger.log(`Metrics reset for queue: ${queueName}`);
  }

  resetAllMetrics(): void {
    this.queueMetrics.clear();
    this.workerMetrics.clear();
    this.logger.log('All metrics reset');
  }

  getPrometheusMetrics(): string {
    const lines: string[] = [];

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

    return lines.join('\n');
  }

  private getOrCreateQueueMetrics(queueName: string): QueueMetricsData {
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
        averageWaitTime: 0,
      };
      this.queueMetrics.set(queueName, metrics);
    }

    return metrics;
  }

  private getOrCreateWorkerMetrics(workerId: string): WorkerMetrics {
    let metrics = this.workerMetrics.get(workerId);

    if (!metrics) {
      metrics = {
        jobsProcessed: 0,
        jobsSucceeded: 0,
        jobsFailed: 0,
        durations: [],
      };
      this.workerMetrics.set(workerId, metrics);
    }

    return metrics;
  }
}