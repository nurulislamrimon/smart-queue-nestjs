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
}

export interface SmartQueueModuleOptions {
  connection?: SmartQueueConnectionOptions;
  defaultJobOptions?: JobsOptions;
  defaultWorkerOptions?: WorkerOptions;
  enableAutoInject?: boolean;
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

export interface JobEventHandlers {
  completed?: (jobId: string, result: unknown) => void;
  failed?: (jobId: string, error: Error) => void;
  progress?: (jobId: string, progress: number | object) => void;
  waiting?: (jobId: string) => void;
  stalled?: (jobId: string) => void;
}

export interface RetryStrategyOptions {
  attempts: number;
  backoff?: number | { type: 'exponential' | 'fixed'; delay: number };
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

export const SMART_QUEUE_MODULE_OPTIONS = 'SMART_QUEUE_MODULE_OPTIONS';
export const QUEUE_SERVICE_TOKEN = 'QUEUE_SERVICE';
export const QUEUE_REGISTRY_TOKEN = 'QUEUE_REGISTRY';
