import { QueueMetricsService, QueueMetricsData } from './queue-metrics.service';

describe('QueueMetricsService', () => {
  let service: QueueMetricsService;

  beforeEach(() => {
    service = new QueueMetricsService();
  });

  describe('recordJobCreated', () => {
    it('should increment jobs created count', () => {
      service.recordJobCreated('test-queue');
      const metrics = service.getQueueMetrics('test-queue');

      expect(metrics?.jobsCreated).toBe(1);
    });

    it('should handle multiple queues independently', () => {
      service.recordJobCreated('queue-1');
      service.recordJobCreated('queue-1');
      service.recordJobCreated('queue-2');

      const queue1Metrics = service.getQueueMetrics('queue-1');
      const queue2Metrics = service.getQueueMetrics('queue-2');

      expect(queue1Metrics?.jobsCreated).toBe(2);
      expect(queue2Metrics?.jobsCreated).toBe(1);
    });
  });

  describe('recordJobCompleted', () => {
    it('should increment completed count and track duration', () => {
      service.recordJobCreated('test-queue');
      service.recordJobCompleted('test-queue', 100);

      const metrics = service.getQueueMetrics('test-queue');

      expect(metrics?.jobsCompleted).toBe(1);
      expect(metrics?.averageDuration).toBe(100);
    });

    it('should calculate running average duration', () => {
      service.recordJobCreated('test-queue');
      service.recordJobCompleted('test-queue', 100);
      service.recordJobCompleted('test-queue', 200);

      const metrics = service.getQueueMetrics('test-queue');

      expect(metrics?.jobsCompleted).toBe(2);
      expect(metrics?.averageDuration).toBe(150);
    });

    it('should decrement active count', () => {
      service.recordJobActive('test-queue');
      service.recordJobActive('test-queue');
      service.recordJobCompleted('test-queue', 50);

      const metrics = service.getQueueMetrics('test-queue');

      expect(metrics?.jobsActive).toBe(1);
    });
  });

  describe('recordJobFailed', () => {
    it('should increment failed count', () => {
      service.recordJobActive('test-queue');
      service.recordJobFailed('test-queue');

      const metrics = service.getQueueMetrics('test-queue');

      expect(metrics?.jobsFailed).toBe(1);
      expect(metrics?.jobsActive).toBe(0);
    });

    it('should not go below zero on active count', () => {
      service.recordJobFailed('test-queue');
      service.recordJobFailed('test-queue');

      const metrics = service.getQueueMetrics('test-queue');

      expect(metrics?.jobsActive).toBe(0);
    });
  });

  describe('getPrometheusMetrics', () => {
    it('should return formatted Prometheus metrics', () => {
      service.recordJobCreated('test-queue');
      service.recordJobCompleted('test-queue', 100);

      const prometheusOutput = service.getPrometheusMetrics();

      expect(prometheusOutput).toContain('smart_queue_jobs_created_total');
      expect(prometheusOutput).toContain('queue="test-queue"');
      expect(prometheusOutput).toContain('1');
    });

    it('should return empty string for no metrics', () => {
      const prometheusOutput = service.getPrometheusMetrics();

      expect(prometheusOutput).toBe('');
    });
  });

  describe('resetQueueMetrics', () => {
    it('should reset metrics for specific queue', () => {
      service.recordJobCreated('test-queue');
      service.resetQueueMetrics('test-queue');

      const metrics = service.getQueueMetrics('test-queue');

      expect(metrics).toBeUndefined();
    });

    it('should not affect other queues', () => {
      service.recordJobCreated('queue-1');
      service.recordJobCreated('queue-2');
      service.resetQueueMetrics('queue-1');

      const queue1Metrics = service.getQueueMetrics('queue-1');
      const queue2Metrics = service.getQueueMetrics('queue-2');

      expect(queue1Metrics).toBeUndefined();
      expect(queue2Metrics?.jobsCreated).toBe(1);
    });
  });

  describe('resetAllMetrics', () => {
    it('should reset all queue metrics', () => {
      service.recordJobCreated('queue-1');
      service.recordJobCreated('queue-2');
      service.resetAllMetrics();

      expect(service.getQueueMetrics('queue-1')).toBeUndefined();
      expect(service.getQueueMetrics('queue-2')).toBeUndefined();
    });
  });
});