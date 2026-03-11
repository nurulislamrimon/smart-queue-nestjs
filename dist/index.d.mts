import { ModuleMetadata, Type, OnModuleDestroy, NestModule, DynamicModule, MiddlewareConsumer } from '@nestjs/common';
import { JobsOptions, WorkerOptions, Queue as Queue$1, QueueEvents, Worker, Job } from 'bullmq';

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
}
interface SmartQueueModuleOptions {
    connection?: SmartQueueConnectionOptions;
    defaultJobOptions?: JobsOptions;
    defaultWorkerOptions?: WorkerOptions;
    enableAutoInject?: boolean;
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
interface JobEventHandlers {
    completed?: (jobId: string, result: unknown) => void;
    failed?: (jobId: string, error: Error) => void;
    progress?: (jobId: string, progress: number | object) => void;
    waiting?: (jobId: string) => void;
    stalled?: (jobId: string) => void;
}
interface RetryStrategyOptions {
    attempts: number;
    backoff?: number | {
        type: 'exponential' | 'fixed';
        delay: number;
    };
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
declare const SMART_QUEUE_MODULE_OPTIONS = "SMART_QUEUE_MODULE_OPTIONS";
declare const QUEUE_SERVICE_TOKEN = "QUEUE_SERVICE";
declare const QUEUE_REGISTRY_TOKEN = "QUEUE_REGISTRY";

declare const QUEUE_METADATA_KEY = "smart-queue:queue";
declare const PROCESSOR_METADATA_KEY = "smart-queue:processor";
declare const PROCESS_HANDLER_METADATA_KEY = "smart-queue:process";
interface QueueMetadata {
    name: string;
}
interface ProcessorMetadata {
    queue: string;
    concurrency?: number;
}
interface ProcessHandlerMetadata {
    name: string;
    options?: {
        concurrency?: number;
    };
}
declare function Queue(name: string): ClassDecorator;
declare function Processor(queue: string, options?: {
    concurrency?: number;
}): ClassDecorator;
declare function Process(name: string, options?: {
    concurrency?: number;
}): MethodDecorator;
declare function getQueueMetadata(target: Function): QueueMetadata | undefined;
declare function getProcessorMetadata(target: Function): ProcessorMetadata | undefined;
declare function getProcessHandlerMetadata(target: object, propertyKey: string | symbol): ProcessHandlerMetadata | undefined;

declare class QueueRegistry implements OnModuleDestroy {
    private readonly logger;
    private readonly queues;
    private readonly workers;
    private readonly defaultOptions;
    constructor(defaultOptions?: SmartQueueModuleOptions);
    private getConnectionOptions;
    getOrCreateQueue(name: string, options?: Partial<SmartQueueModuleOptions>): Queue$1;
    getQueue(name: string): Queue$1 | undefined;
    getQueueEvents(name: string): QueueEvents | undefined;
    createWorker<T = unknown>(name: string, processor: (job: {
        id: string;
        data: T;
    }) => Promise<unknown>, options?: ProcessorOptions & {
        retryStrategy?: RetryStrategyOptions;
    }): Worker;
    registerEventListeners(queueName: string, handlers: JobEventHandlers): void;
    closeQueue(name: string): Promise<void>;
    closeWorker(name: string): Promise<void>;
    closeAll(): Promise<void>;
    onModuleDestroy(): Promise<void>;
    private getPrefixedQueueName;
}

declare class BullMQAdapter {
    private readonly registry;
    private readonly logger;
    constructor(registry: QueueRegistry);
    getQueue(name: string): Queue$1;
    addJob<T = unknown>(queueName: string, name: string, data: T, options?: QueueAddOptions<T>): Promise<Job>;
    addDelayedJob<T = unknown>(queueName: string, name: string, data: T, delay: number, options?: Omit<QueueDelayOptions<T>, 'delay'>): Promise<Job>;
    addRepeatableJob<T = unknown>(queueName: string, name: string, data: T, cron: string, options?: QueueRepeatOptions<T>): Promise<Job>;
    removeJob(queueName: string, jobId: string): Promise<void>;
    getJob(queueName: string, jobId: string): Promise<Job | null>;
    getMetrics(queueName: string): Promise<QueueMetrics>;
    pauseQueue(queueName: string): Promise<void>;
    resumeQueue(queueName: string): Promise<void>;
    drainQueue(queueName: string): Promise<void>;
    cleanQueue(queueName: string, grace: number, status?: 'completed' | 'failed'): Promise<string[]>;
    createWorker<T = unknown>(queueName: string, processor: (job: {
        id: string;
        data: T;
    }) => Promise<unknown>, options?: ProcessorOptions & {
        retryStrategy?: RetryStrategyOptions;
    }): void;
    registerEventListeners(queueName: string, handlers: JobEventHandlers): void;
    removeRepeatableJob(queueName: string, jobKey: string): Promise<boolean>;
    getRepeatableJobs(queueName: string): Promise<any[]>;
    closeQueue(queueName: string): Promise<void>;
    closeWorker(queueName: string): Promise<void>;
}

declare class QueueService {
    private readonly adapter;
    private readonly moduleOptions?;
    private readonly logger;
    private readonly defaultQueueOptions;
    constructor(adapter: BullMQAdapter, moduleOptions?: SmartQueueModuleOptions | undefined);
    add<T = unknown>(queueName: string, name: string, data: T, options?: QueueAddOptions<T>): Promise<Job>;
    delay<T = unknown>(queueName: string, name: string, data: T, delay: number, options?: Omit<QueueDelayOptions<T>, 'delay'>): Promise<Job<T>>;
    repeat<T = unknown>(queueName: string, name: string, data: T, cron: string, options?: QueueRepeatOptions<T>): Promise<Job<T>>;
    remove(queueName: string, jobId: string): Promise<void>;
    getJob(queueName: string, jobId: string): Promise<Job | null>;
    getMetrics(queueName: string): Promise<QueueMetrics>;
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
    close(queueName: string): Promise<void>;
}

declare class SmartQueueModule implements NestModule {
    constructor();
    static forRoot(options?: SmartQueueModuleOptions): DynamicModule;
    static forRootAsync(options: SmartQueueModuleAsyncOptions): DynamicModule;
    private static createProviders;
    private static createAsyncProviders;
    configure(consumer: MiddlewareConsumer): void;
}
interface SmartQueueModuleConfig extends ModuleMetadata {
    options?: SmartQueueModuleOptions;
}

export { BullMQAdapter, type JobEventHandlers, PROCESSOR_METADATA_KEY, PROCESS_HANDLER_METADATA_KEY, Process, type ProcessHandlerMetadata, Processor, type ProcessorMetadata, type ProcessorOptions, QUEUE_METADATA_KEY, QUEUE_REGISTRY_TOKEN, QUEUE_SERVICE_TOKEN, Queue, type QueueAddOptions, type QueueDelayOptions, type QueueMetadata, type QueueMetrics, QueueRegistry, type QueueRepeatOptions, QueueService, type RateLimitOptions, type RetryStrategyOptions, SMART_QUEUE_MODULE_OPTIONS, type SmartQueueConnectionOptions, SmartQueueModule, type SmartQueueModuleAsyncOptions, type SmartQueueModuleConfig, type SmartQueueModuleOptions, type SmartQueueOptionsFactory, getProcessHandlerMetadata, getProcessorMetadata, getQueueMetadata };
