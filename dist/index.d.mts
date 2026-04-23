import { ModuleMetadata, Type, OnModuleDestroy, OnModuleInit, NestModule, DynamicModule, MiddlewareConsumer } from '@nestjs/common';
import { JobsOptions, WorkerOptions, Queue as Queue$1, QueueEvents, Worker, Job } from 'bullmq';
import { Module } from '@nestjs/core/injector/module';
import { MetadataScanner } from '@nestjs/core/metadata-scanner';

interface SmartQueueConnectionOptions {
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
interface SmartQueueTlsOptions {
    enabled?: boolean;
    ca?: string;
    cert?: string;
    key?: string;
    rejectUnauthorized?: boolean;
}
interface SmartQueueModuleOptions {
    connection?: SmartQueueConnectionOptions;
    defaultJobOptions?: JobsOptions;
    defaultWorkerOptions?: WorkerOptions;
    enableAutoInject?: boolean;
    prefix?: string;
}
interface SmartQueueOptionsFactory {
    createSmartQueueOptions(): Promise<SmartQueueModuleOptions> | SmartQueueModuleOptions;
}
interface SmartQueueModuleAsyncOptions extends Pick<ModuleMetadata, 'imports' | 'providers'> {
    useExisting?: Type<SmartQueueOptionsFactory>;
    useClass?: Type<SmartQueueOptionsFactory>;
    useFactory?: (...args: unknown[]) => Promise<SmartQueueModuleOptions> | SmartQueueModuleOptions;
    inject?: Type<unknown>[];
}
interface QueueAddOptions<T = unknown> {
    name?: string;
    data: T;
    jobId?: string;
    priority?: number;
    delay?: number;
    attempts?: number;
    backoff?: number | {
        type: string;
        delay: number;
    };
    lifo?: boolean;
    timeout?: number;
    removeOnComplete?: boolean | number;
    removeOnFail?: boolean | number;
    repeatJobKey?: string;
}
interface QueueDelayOptions<T = unknown> {
    name?: string;
    data: T;
    delay: number;
    jobId?: string | number;
    attempts?: number;
    backoff?: number | {
        type: string;
        delay: number;
    };
}
interface QueueRepeatOptions<T = unknown> {
    name?: string;
    data: T;
    cron: string;
    jobId?: string | number;
    tz?: string;
    startDate?: Date | string | number;
    endDate?: Date | string | number;
    limit?: number;
}
interface QueueMetrics {
    waiting: number;
    active: number;
    completed: number;
    failed: number;
    delayed: number;
    paused: number;
}
interface QueueHealthCheck {
    isHealthy: boolean;
    queueName: string;
    connectionStatus: 'connected' | 'disconnected' | 'error';
    lastCheck: Date;
    error?: string;
    jobCounts: QueueMetrics;
}
interface JobEventHandlers {
    completed?: (jobId: string, result: unknown) => void;
    failed?: (jobId: string, error: Error) => void;
    progress?: (jobId: string, progress: number | object) => void;
    waiting?: (jobId: string) => void;
    stalled?: (jobId: string) => void;
}
interface RetryStrategyOptions {
    maxAttempts: number;
    backoff?: number | {
        type: 'exponential' | 'fixed';
        delay: number;
    };
    enabled?: boolean;
}
interface BackoffStrategy {
    type: 'exponential' | 'fixed';
    delay: number;
}
interface RateLimitOptions {
    max: number;
    duration: number;
    groupKey?: string;
}
interface ProcessorOptions {
    queue: string;
    concurrency?: number;
    limiter?: RateLimitOptions;
    lockDuration?: number;
    lockRenewTime?: number;
    maxStalledCount?: number;
    stalledInterval?: number;
}
interface DeadLetterQueueOptions {
    enabled: boolean;
    maxRetries?: number;
    queueName?: string;
}
interface JobHooks {
    beforeJob?: (jobId: string, data: unknown) => Promise<unknown> | unknown;
    afterJob?: (jobId: string, result: unknown) => Promise<void> | void;
    onJobFailed?: (jobId: string, error: Error) => Promise<void> | void;
}
interface JobHandlerOptions {
    name: string;
    queue?: string;
    concurrency?: number;
    retryStrategy?: RetryStrategyOptions;
    deadLetterQueue?: DeadLetterQueueOptions;
    hooks?: JobHooks;
}
interface SmartQueueJob<T = unknown> {
    id: string;
    name: string;
    data: T;
    attemptsMade: number;
    progress: number | object;
}
interface JobLoggerContext {
    jobId: string;
    jobName: string;
    queueName: string;
    attempt?: number;
}
declare const SMART_QUEUE_MODULE_OPTIONS = "SMART_QUEUE_MODULE_OPTIONS";
declare const QUEUE_SERVICE_TOKEN = "QUEUE_SERVICE";
declare const QUEUE_REGISTRY_TOKEN = "QUEUE_REGISTRY";
declare const QUEUE_HEALTH_INDICATOR = "QUEUE_HEALTH_INDICATOR";

interface QueueMetadata {
    name: string;
}
interface ProcessorMetadata {
    queue: string;
    concurrency?: number;
}
interface QueueHandlerMetadata {
    name: string;
    options?: {
        queue?: string;
        concurrency?: number;
        retryStrategy?: {
            maxAttempts: number;
            backoff?: {
                type: 'exponential' | 'fixed';
                delay: number;
            };
        };
        deadLetterQueue?: {
            enabled: boolean;
            maxRetries?: number;
            queueName?: string;
        };
    };
}

declare const QUEUE_METADATA_KEY = "smart-queue:queue";
declare const PROCESSOR_METADATA_KEY = "smart-queue:processor";
declare const QUEUE_HANDLER_METADATA_KEY = "smart-queue:handler";
declare function Queue(name: string): ClassDecorator;
declare function Processor(queue: string, options?: {
    concurrency?: number;
}): ClassDecorator;
declare function QueueHandler(name: string, options?: {
    queue?: string;
    concurrency?: number;
    retryStrategy?: {
        maxAttempts: number;
        backoff?: {
            type: 'exponential' | 'fixed';
            delay: number;
        };
    };
    deadLetterQueue?: {
        enabled: boolean;
        maxRetries?: number;
        queueName?: string;
    };
}): MethodDecorator;
declare function getQueueMetadata(target: Function): QueueMetadata | undefined;
declare function getProcessorMetadata(target: Function): ProcessorMetadata | undefined;
declare function getQueueHandlerMetadata(target: object, propertyKey: string | symbol): QueueHandlerMetadata | undefined;

interface QueueMetricsData {
    jobsCreated: number;
    jobsCompleted: number;
    jobsFailed: number;
    jobsActive: number;
    jobsWaiting: number;
    jobsDelayed: number;
    averageDuration: number;
    averageWaitTime: number;
}
interface WorkerMetrics {
    jobsProcessed: number;
    jobsSucceeded: number;
    jobsFailed: number;
    durations: number[];
}
declare class QueueMetricsService {
    private readonly logger;
    private readonly queueMetrics;
    private readonly workerMetrics;
    private static readonly METRIC_NAMES;
    recordJobCreated(queueName: string): void;
    recordJobCompleted(queueName: string, durationMs: number): void;
    recordJobFailed(queueName: string): void;
    recordJobActive(queueName: string): void;
    recordJobWaiting(queueName: string, count: number): void;
    recordJobDelayed(queueName: string, count: number): void;
    getQueueMetrics(queueName: string): QueueMetricsData | undefined;
    getAllMetrics(): Map<string, QueueMetricsData>;
    resetQueueMetrics(queueName: string): void;
    resetAllMetrics(): void;
    getPrometheusMetrics(): string;
    private getOrCreateQueueMetrics;
    private getOrCreateWorkerMetrics;
}

declare class QueueRegistry implements OnModuleDestroy, OnModuleInit {
    private readonly metricsService?;
    private readonly logger;
    private readonly queues;
    private readonly workers;
    private readonly queueConfigs;
    private readonly defaultOptions;
    private readonly jobHooks;
    private isInitialized;
    constructor(defaultOptions?: SmartQueueModuleOptions, metricsService?: QueueMetricsService | undefined);
    onModuleInit(): Promise<void>;
    private getConnectionOptions;
    registerQueue(name: string, options?: Partial<SmartQueueModuleOptions>): void;
    registerDeadLetterQueue(mainQueueName: string, options: DeadLetterQueueOptions): void;
    getOrCreateQueue(name: string): Queue$1;
    getQueue(name: string): Queue$1 | undefined;
    getDeadLetterQueue(name: string): Queue$1 | undefined;
    getQueueEvents(name: string): QueueEvents | undefined;
    getRegisteredQueueNames(): string[];
    registerJobHooks(queueName: string, hooks: JobHooks): void;
    getJobHooks(queueName: string): JobHooks | undefined;
    createWorker<T = unknown>(name: string, processor: (job: {
        id: string;
        data: T;
    }) => Promise<unknown>, options?: ProcessorOptions & {
        retryStrategy?: RetryStrategyOptions;
    }): Worker;
    private calculateBackoff;
    private moveToDeadLetterQueue;
    registerEventListeners(queueName: string, handlers: JobEventHandlers): void;
    closeQueue(name: string): Promise<void>;
    closeWorker(name: string): Promise<void>;
    closeAll(): Promise<void>;
    onModuleDestroy(): Promise<void>;
    private getPrefixedQueueName;
}

declare class BullMQAdapter implements OnModuleDestroy {
    private readonly registry;
    private readonly metricsService?;
    private readonly logger;
    constructor(registry: QueueRegistry, metricsService?: QueueMetricsService | undefined);
    registerQueue(name: string, options?: any): void;
    registerDeadLetterQueue(name: string, options: DeadLetterQueueOptions): void;
    registerJobHooks(name: string, hooks: JobHooks): void;
    getQueue(name: string): Queue$1;
    addJob<T = unknown>(queueName: string, name: string, data: T, options?: QueueAddOptions<T>): Promise<Job>;
    addDelayedJob<T = unknown>(queueName: string, name: string, data: T, delay: number, options?: Omit<QueueDelayOptions<T>, 'delay'>): Promise<Job>;
    addRepeatableJob<T = unknown>(queueName: string, name: string, data: T, cron: string, options?: QueueRepeatOptions<T>): Promise<Job>;
    removeJob(queueName: string, jobId: string): Promise<void>;
    getJob(queueName: string, jobId: string): Promise<Job | null>;
    getQueueMetrics(queueName: string): Promise<QueueMetrics>;
    pauseQueue(queueName: string): Promise<void>;
    resumeQueue(queueName: string): Promise<void>;
    drainQueue(queueName: string): Promise<void>;
    cleanQueue(queueName: string, grace: number, status?: 'completed' | 'failed'): Promise<string[]>;
    createWorker<T = unknown>(queueName: string, processor: (job: {
        id: string;
        data: T;
    }) => Promise<unknown>, options?: ProcessorOptions & {
        retryStrategy?: any;
    }): void;
    registerEventListeners(queueName: string, handlers: JobEventHandlers): void;
    removeRepeatableJob(queueName: string, jobKey: string): Promise<boolean>;
    getRepeatableJobs(queueName: string): Promise<any[]>;
    closeQueue(queueName: string): Promise<void>;
    closeWorker(queueName: string): Promise<void>;
    getQueueNames(): string[];
    onModuleDestroy(): Promise<void>;
}

declare class QueueHealthService implements OnModuleDestroy {
    private readonly registry;
    private readonly logger;
    private readonly healthChecks;
    private healthCheckInterval;
    private readonly checkIntervalMs;
    constructor(registry: QueueRegistry);
    checkQueueHealth(queueName: string): Promise<QueueHealthCheck>;
    checkAllQueuesHealth(): Promise<Map<string, QueueHealthCheck>>;
    isHealthy(queueName: string): Promise<boolean>;
    getLastHealthCheck(queueName: string): QueueHealthCheck | undefined;
    startHealthCheckMonitor(intervalMs?: number): void;
    stopHealthCheckMonitor(): void;
    private checkRedisConnection;
    private getJobCounts;
    onModuleDestroy(): Promise<void>;
}

interface QueueJob<T = unknown> {
    id: string;
    name: string;
    data: T;
    progress: number | object;
}
declare class QueueService implements OnModuleDestroy {
    private readonly adapter;
    private readonly healthService;
    private readonly metricsService;
    private readonly logger;
    private readonly defaultQueueOptions;
    constructor(adapter: BullMQAdapter, healthService: QueueHealthService, metricsService: QueueMetricsService, moduleOptions?: SmartQueueModuleOptions);
    registerQueue(name: string, options?: Partial<SmartQueueModuleOptions>): void;
    registerDeadLetterQueue(queueName: string, options: DeadLetterQueueOptions): void;
    registerJobHooks(queueName: string, hooks: JobHooks): void;
    add<T = unknown>(queueName: string, name: string, data: T, options?: QueueAddOptions<T>): Promise<Job>;
    addWithIdempotencyKey<T = unknown>(queueName: string, name: string, data: T, idempotencyKey: string, options?: QueueAddOptions<T>): Promise<Job>;
    delay<T = unknown>(queueName: string, name: string, data: T, delay: number, options?: Omit<QueueDelayOptions<T>, 'delay'>): Promise<Job<T>>;
    repeat<T = unknown>(queueName: string, name: string, data: T, cron: string, options?: QueueRepeatOptions<T>): Promise<Job<T>>;
    remove(queueName: string, jobId: string): Promise<void>;
    getJob(queueName: string, jobId: string): Promise<Job | null>;
    getQueueMetrics(queueName: string): Promise<QueueMetrics>;
    pause(queueName: string): Promise<void>;
    resume(queueName: string): Promise<void>;
    drain(queueName: string): Promise<void>;
    clean(queueName: string, grace?: number, status?: 'completed' | 'failed'): Promise<string[]>;
    removeRepeatable(queueName: string, jobKey: string): Promise<boolean>;
    getRepeatableJobs(queueName: string): Promise<Job[]>;
    worker<T = unknown>(queueName: string, processor: (job: {
        id: string;
        data: T;
    }) => Promise<unknown>, options?: ProcessorOptions & {
        retryStrategy?: RetryStrategyOptions;
    }): void;
    on<T = unknown>(queueName: string, event: 'completed' | 'failed' | 'progress' | 'waiting' | 'stalled', handler: (...args: unknown[]) => void): void;
    checkHealth(queueName: string): Promise<QueueHealthCheck>;
    checkAllHealth(): Promise<Map<string, QueueHealthCheck>>;
    isHealthy(queueName: string): Promise<boolean>;
    getMetrics(): string;
    close(queueName: string): Promise<void>;
    onModuleDestroy(): Promise<void>;
}

declare class ProcessorScannerService implements OnModuleInit {
    private readonly queueService;
    private readonly metadataScanner;
    private readonly logger;
    private readonly scannedQueues;
    constructor(queueService: QueueService, metadataScanner: MetadataScanner);
    onModuleInit(): Promise<void>;
    scanAndRegister(modules: Module[]): void;
    scanModule(module: Module): void;
    private isProcessorClass;
    private registerProcessorClass;
}

declare class SmartQueueModule implements NestModule, OnModuleDestroy {
    private readonly logger;
    constructor();
    static forRoot(options?: SmartQueueModuleOptions): DynamicModule;
    static forRootAsync(options: SmartQueueModuleAsyncOptions): DynamicModule;
    static forFeature(queueNames: string[]): DynamicModule;
    private static createProviders;
    private static createAsyncProviders;
    configure(consumer: MiddlewareConsumer): void;
    onModuleDestroy(): Promise<void>;
}
interface SmartQueueModuleConfig extends ModuleMetadata {
    options?: SmartQueueModuleOptions;
}

interface BullBoardOptions {
    routePath?: string;
    apiRoute?: string;
}
declare class BullBoardModule implements OnModuleInit {
    private queueRegistry;
    static forRoot(options?: BullBoardOptions): DynamicModule;
    constructor(queueRegistry: QueueRegistry);
    onModuleInit(): Promise<void>;
    static getQueues(queueRegistry: QueueRegistry): any[];
}

export { type BackoffStrategy, BullBoardModule, type BullBoardOptions, BullMQAdapter, type DeadLetterQueueOptions, type JobEventHandlers, type JobHandlerOptions, type JobHooks, type JobLoggerContext, PROCESSOR_METADATA_KEY, Processor, type ProcessorMetadata, type ProcessorOptions, ProcessorScannerService, QUEUE_HANDLER_METADATA_KEY, QUEUE_HEALTH_INDICATOR, QUEUE_METADATA_KEY, QUEUE_REGISTRY_TOKEN, QUEUE_SERVICE_TOKEN, Queue, type QueueAddOptions, type QueueDelayOptions, QueueHandler, type QueueHandlerMetadata, type QueueHealthCheck, QueueHealthService, type QueueJob, type QueueMetadata, type QueueMetrics, type QueueMetricsData, QueueMetricsService, QueueRegistry, type QueueRepeatOptions, QueueService, type RateLimitOptions, type RetryStrategyOptions, SMART_QUEUE_MODULE_OPTIONS, type SmartQueueConnectionOptions, type SmartQueueJob, SmartQueueModule, type SmartQueueModuleAsyncOptions, type SmartQueueModuleConfig, type SmartQueueModuleOptions, type SmartQueueOptionsFactory, type SmartQueueTlsOptions, type WorkerMetrics, getProcessorMetadata, getQueueHandlerMetadata, getQueueMetadata };
