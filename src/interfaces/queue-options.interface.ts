import { ModuleMetadata, Type } from '@nestjs/common';
import { QueueOptions, WorkerOptions, JobsOptions } from 'bullmq';

export interface SmartQueueConnectionOptions {
  host?: string;
  port?: number;
  db?: number;
  password?: string;
  family?: number;
  keyPrefix?: string;
  connectTimeout?: number;
  maxRetriesPerRequest?: number;
  enableOfflineQueue?: boolean;
  retryStrategy?: (times: number) => number | null;
  tls?: SmartQueueTlsOptions;
}

export interface SmartQueueTlsOptions {
  enabled?: boolean;
  ca?: string;
  cert?: string;
  key?: string;
  rejectUnauthorized?: boolean;
}

export interface SmartQueueModuleOptions {
  connection?: SmartQueueConnectionOptions;
  defaultJobOptions?: JobsOptions;
  defaultWorkerOptions?: WorkerOptions;
  enableAutoInject?: boolean;
  prefix?: string;
}

export interface SmartQueueOptionsFactory {
  createSmartQueueOptions(): Promise<SmartQueueModuleOptions> | SmartQueueModuleOptions;
}

export interface SmartQueueModuleAsyncOptions
  extends Pick<ModuleMetadata, 'imports' | 'providers'> {
  useExisting?: Type<SmartQueueOptionsFactory>;
  useClass?: Type<SmartQueueOptionsFactory>;
  useFactory?: (
    ...args: unknown[]
  ) => Promise<SmartQueueModuleOptions> | SmartQueueModuleOptions;
  inject?: Type<unknown>[];
}

export interface QueueAddOptions<T = unknown> {
  name?: string;
  data: T;
  jobId?: string;
  priority?: number;
  delay?: number;
  attempts?: number;
  backoff?: number | { type: string; delay: number };
  lifo?: boolean;
  timeout?: number;
  removeOnComplete?: boolean | number;
  removeOnFail?: boolean | number;
  repeatJobKey?: string;
}

export interface QueueDelayOptions<T = unknown> {
  name?: string;
  data: T;
  delay: number;
  jobId?: string | number;
  attempts?: number;
  backoff?: number | { type: string; delay: number };
}

export interface QueueRepeatOptions<T = unknown> {
  name?: string;
  data: T;
  cron: string;
  jobId?: string | number;
  tz?: string;
  startDate?: Date | string | number;
  endDate?: Date | string | number;
  limit?: number;
}

export interface QueueMetrics {
  waiting: number;
  active: number;
  completed: number;
  failed: number;
  delayed: number;
  paused: number;
}

export interface QueueHealthCheck {
  isHealthy: boolean;
  queueName: string;
  connectionStatus: 'connected' | 'disconnected' | 'error';
  lastCheck: Date;
  error?: string;
  jobCounts: QueueMetrics;
}

export interface JobEventHandlers {
  completed?: (jobId: string, result: unknown) => void;
  failed?: (jobId: string, error: Error) => void;
  progress?: (jobId: string, progress: number | object) => void;
  waiting?: (jobId: string) => void;
  stalled?: (jobId: string) => void;
}

export interface RetryStrategyOptions {
  maxAttempts: number;
  backoff?: number | { type: 'exponential' | 'fixed'; delay: number };
  enabled?: boolean;
}

export interface BackoffStrategy {
  type: 'exponential' | 'fixed';
  delay: number;
}

export interface RateLimitOptions {
  max: number;
  duration: number;
  groupKey?: string;
}

export interface ProcessorOptions {
  queue: string;
  concurrency?: number;
  limiter?: RateLimitOptions;
  lockDuration?: number;
  lockRenewTime?: number;
  maxStalledCount?: number;
  stalledInterval?: number;
}

export interface DeadLetterQueueOptions {
  enabled: boolean;
  maxRetries?: number;
  queueName?: string;
}

export interface JobHooks {
  beforeJob?: (jobId: string, data: unknown) => Promise<unknown> | unknown;
  afterJob?: (jobId: string, result: unknown) => Promise<void> | void;
  onJobFailed?: (jobId: string, error: Error) => Promise<void> | void;
}

export interface JobHandlerOptions {
  name: string;
  queue?: string;
  concurrency?: number;
  retryStrategy?: RetryStrategyOptions;
  deadLetterQueue?: DeadLetterQueueOptions;
  hooks?: JobHooks;
}

export interface SmartQueueJob<T = unknown> {
  id: string;
  name: string;
  data: T;
  attemptsMade: number;
  progress: number | object;
}

export interface JobLoggerContext {
  jobId: string;
  jobName: string;
  queueName: string;
  attempt?: number;
}

export const SMART_QUEUE_MODULE_OPTIONS = 'SMART_QUEUE_MODULE_OPTIONS';
export const QUEUE_SERVICE_TOKEN = 'QUEUE_SERVICE';
export const QUEUE_REGISTRY_TOKEN = 'QUEUE_REGISTRY';
export const QUEUE_HEALTH_INDICATOR = 'QUEUE_HEALTH_INDICATOR';