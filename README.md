# smart-queue-nestjs

<p align="center">
  <a href="https://www.npmjs.com/package/smart-queue-nestjs">
    <img src="https://img.shields.io/npm/v/smart-queue-nestjs.svg" alt="NPM Version">
  </a>
  <a href="https://github.com/nurulislamrimon/smart-queue-nestjs/blob/main/LICENSE">
    <img src="https://img.shields.io/github/license/nurulislamrimon/smart-queue-nestjs.svg" alt="License">
  </a>
  <a href="https://github.com/nurulislamrimon/smart-queue-nestjs/actions">
    <img src="https://github.com/nurulislamrimon/smart-queue-nestjs/workflows/Test/badge.svg" alt="Build Status">
  </a>
</p>

<p align="center">
  Enterprise-grade BullMQ integration for NestJS applications with production-ready features
</p>

---

## Features

- **Production-Ready**: Fault tolerance, retry strategies, dead-letter queues
- **Observability**: Structured logging, Prometheus metrics, health checks
- **Type-Safe**: Full TypeScript support with generics
- **Scalable**: Horizontal scaling with configurable concurrency
- **Developer Experience**: Clean decorators, async configuration

## Installation

```bash
npm install smart-queue-nestjs bullmq ioredis
```

## Quick Start

### 1. Configure the Module

```typescript
import { Module } from '@nestjs/common';
import { SmartQueueModule } from 'smart-queue-nestjs';

@Module({
  imports: [
    SmartQueueModule.forRoot({
      connection: {
        host: 'localhost',
        port: 6379,
      },
      defaultJobOptions: {
        attempts: 3,
        backoff: {
          type: 'exponential',
          delay: 1000,
        },
      },
    }),
  ],
  exports: [SmartQueueModule],
})
export class AppModule {}
```

### 2. Create a Producer Service

```typescript
import { Injectable } from '@nestjs/common';
import { QueueService } from 'smart-queue-nestjs';

interface EmailJobData {
  to: string;
  subject: string;
  body: string;
}

@Injectable()
export class EmailProducerService {
  constructor(private readonly queue: QueueService) {}

  async sendEmail(data: EmailJobData) {
    const job = await this.queue.add('emails', 'send', data, {
      attempts: 3,
      backoff: { type: 'exponential', delay: 1000 },
    });
    
    return job;
  }
}
```

### 3. Create a Consumer/Worker

```typescript
import { Injectable, Logger } from '@nestjs/common';
import { QueueService } from 'smart-queue-nestjs';

interface EmailJobData {
  to: string;
  subject: string;
  body: string;
}

@Injectable()
export class EmailWorkerService {
  private readonly logger = new Logger(EmailWorkerService.name);

  constructor(private readonly queue: QueueService) {
    this.registerWorker();
  }

  private registerWorker() {
    this.queue.worker<EmailJobData>(
      'emails',
      async ({ id, data }) => {
        this.logger.log(`Processing email job ${id}`);
        
        // Send email logic here
        await this.sendEmail(data);
        
        return { success: true, jobId: id };
      },
      {
        concurrency: 5,
        retryStrategy: {
          maxAttempts: 3,
          backoff: { type: 'exponential', delay: 1000 },
        },
      },
    );
  }

  private async sendEmail(data: EmailJobData): Promise<void> {
    // Email sending implementation
  }
}
```

---

## Advanced Usage

### Async Configuration

```typescript
import { Module } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { SmartQueueModule } from 'smart-queue-nestjs';

@Module({
  imports: [
    SmartQueueModule.forRootAsync({
      useFactory: (config: ConfigService) => ({
        connection: {
          host: config.get('REDIS_HOST'),
          port: config.get('REDIS_PORT'),
          password: config.get('REDIS_PASSWORD'),
        },
      }),
      inject: [ConfigService],
    }),
  ],
})
export class AppModule {}
```

### Type-Safe Job Payloads

```typescript
// Define your job data types
interface ProcessOrderData {
  orderId: string;
  items: Array<{ productId: string; quantity: number }>;
  customerEmail: string;
}

interface ProcessOrderResult {
  orderId: string;
  status: 'processed' | 'failed';
  processedAt: string;
}

// Producer
const job = await queue.add<OrderData>('orders', 'process', {
  orderId: 'ORD-123',
  items: [{ productId: 'PROD-1', quantity: 2 }],
  customerEmail: 'customer@example.com',
});

// Worker with full type safety
queue.worker<OrderData>(
  'orders',
  async ({ id, data }) => {
    const result = await processOrder(data);
    return result; // Fully typed
  },
);
```

### Retry Strategies

```typescript
// Exponential backoff (default)
this.queue.add('jobs', 'process', data, {
  attempts: 5,
  backoff: {
    type: 'exponential',
    delay: 1000, // 1s, 2s, 4s, 8s, 16s
  },
});

// Fixed delay
this.queue.add('jobs', 'process', data, {
  attempts: 3,
  backoff: {
    type: 'fixed',
    delay: 5000, // Always 5s between retries
  },
});

// Number shorthand (fixed delay in ms)
this.queue.add('jobs', 'process', data, {
  attempts: 3,
  backoff: 2000,
});
```

### Dead Letter Queue (DLQ)

```typescript
@Injectable()
export class OrderProcessorService {
  constructor(private readonly queue: QueueService) {
    this.setupQueue();
  }

  private setupQueue() {
    // Enable DLQ for failed jobs after max retries exhausted
    this.queue.registerDeadLetterQueue('orders', {
      enabled: true,
      queueName: 'orders-dlq',
      maxRetries: 3,
    });

    this.queue.worker<OrderData>(
      'orders',
      async ({ id, data }) => {
        // Process order
      },
      {
        retryStrategy: {
          maxAttempts: 3,
          backoff: { type: 'exponential', delay: 1000 },
        },
      },
    );
  }

  // Process DLQ jobs separately
  async reprocessFailedOrders() {
    const failedJobs = await this.queue.getRepeatableJobs('orders-dlq');
    // Reprocess logic
  }
}
```

### Idempotent Job Addition

```typescript
// Prevent duplicate jobs using idempotency keys
async addPaymentProcessing(orderId: string, amount: number) {
  const idempotencyKey = `payment-${orderId}-${Date.now()}`;
  
  await this.queue.addWithIdempotencyKey(
    'payments',
    'process',
    { orderId, amount },
    idempotencyKey, // Same key = same job, no duplicates
  );
}
```

### Job Hooks

```typescript
this.queue.registerJobHooks('orders', {
  beforeJob: async (jobId, data) => {
    console.log(`Starting job ${jobId}`);
    // Validation, logging, etc.
    return data;
  },
  afterJob: async (jobId, result) => {
    console.log(`Job ${jobId} completed with result:`, result);
    // Notifications, cleanup, etc.
  },
  onJobFailed: async (jobId, error) => {
    console.log(`Job ${jobId} failed:`, error.message);
    // Alerting, cleanup, etc.
  },
});
```

### Delayed Jobs

```typescript
// Execute after 5 minutes
await this.queue.delay('notifications', 'send', data, 5 * 60 * 1000);

// Schedule with cron
await this.queue.repeat('reports', 'generate', data, '0 0 * * *'); // Daily at midnight
```

### Health Checks

```typescript
@Controller('health')
export class HealthController {
  constructor(private readonly queue: QueueService) {}

  @Get('queue')
  async checkQueueHealth() {
    const health = await this.queue.checkHealth('orders');
    return {
      queue: health.queueName,
      isHealthy: health.isHealthy,
      connectionStatus: health.connectionStatus,
      jobCounts: health.jobCounts,
    };
  }

  @Get('queue/all')
  async checkAllQueues() {
    return this.queue.checkAllHealth();
  }
}
```

### Prometheus Metrics

```typescript
@Controller('metrics')
export class MetricsController {
  constructor(private readonly queue: QueueService) {}

  @Get()
  getMetrics() {
    return this.queue.getMetrics();
  }
}
```

Returns Prometheus-format output:
```
# TYPE smart_queue_jobs_created_total counter
smart_queue_jobs_created_total{queue="orders"} 150
# TYPE smart_queue_jobs_completed_total counter
smart_queue_jobs_completed_total{queue="orders"} 145
...
```

---

## Configuration Options

### SmartQueueConnectionOptions

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `host` | `string` | `'localhost'` | Redis host |
| `port` | `number` | `6379` | Redis port |
| `db` | `number` | `0` | Redis database |
| `password` | `string` | - | Redis password |
| `keyPrefix` | `string` | `'smart-queue'` | Key prefix |
| `tls.enabled` | `boolean` | `false` | Enable TLS |
| `tls.ca` | `string` | - | CA certificate |
| `tls.cert` | `string` | - | Client certificate |
| `tls.key` | `string` | - | Client key |

### ProcessorOptions

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `queue` | `string` | - | Queue name |
| `concurrency` | `number` | `1` | Concurrent jobs |
| `limiter.max` | `number` | - | Rate limit max |
| `limiter.duration` | `number` | - | Rate limit duration |
| `lockDuration` | `number` | `30000` | Job lock duration |

---

## Production Setup

### High Availability

```typescript
SmartQueueModule.forRoot({
  connection: {
    host: process.env.REDIS_HOST,
    port: parseInt(process.env.REDIS_PORT),
    password: process.env.REDIS_PASSWORD,
    // Connection pool for high load
    maxRetriesPerRequest: 3,
    enableOfflineQueue: false,
    retryStrategy: (times) => {
      if (times > 10) return null; // Stop retrying
      return Math.min(times * 200, 2000);
    },
  },
  defaultJobOptions: {
    attempts: 5,
    backoff: { type: 'exponential', delay: 1000 },
    removeOnComplete: 100,
    removeOnFail: 500,
  },
});
```

### Graceful Shutdown

The module automatically handles graceful shutdown via `onModuleDestroy`:

- Pauses new job processing
- Waits for active jobs to complete
- Closes connections cleanly

For custom handling:

```typescript
async onModuleDestroy() {
  // Stop accepting new jobs
  await this.queue.pause('orders');
  
  // Wait for active jobs (up to 30s)
  await this.delay(30000);
  
  // Force close
  await this.queue.close('orders');
}
```

---

## API Reference

### QueueService

```typescript
class QueueService {
  // Job Management
  add<T>(queue, name, data, options): Promise<Job>
  addWithIdempotencyKey<T>(queue, name, data, key, options): Promise<Job>
  delay<T>(queue, name, data, delay, options): Promise<Job>
  repeat<T>(queue, name, data, cron, options): Promise<Job>
  
  // Job Queries
  getJob(queue, jobId): Promise<Job | null>
  getMetrics(queue): Promise<QueueMetrics>
  getRepeatableJobs(queue): Promise<Job[]>
  
  // Queue Control
  pause(queue): Promise<void>
  resume(queue): Promise<void>
  drain(queue): Promise<void>
  clean(queue, grace, status): Promise<string[]>
  close(queue): Promise<void>
  
  // Worker
  worker<T>(queue, processor, options): void
  on(queue, event, handler): void
  
  // Health & Metrics
  checkHealth(queue): Promise<QueueHealthCheck>
  checkAllHealth(): Promise<Map<string, QueueHealthCheck>>
  getMetrics(): string
  
  // Configuration
  registerQueue(name, options): void
  registerDeadLetterQueue(name, options): void
  registerJobHooks(name, hooks): void
}
```

---

### Bull Board Dashboard (Optional)

For queue monitoring, install `@bull-board/api`:

```bash
npm install @bull-board/api
```

Then create a controller:

```typescript
import { Controller, Get, Module } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { createBullBoard } from '@bull-board/api';
import { BullAdapter } from '@bull-board/api';
import { QueueRegistry } from 'smart-queue-nestjs';

@Controller('admin/queues')
export class QueueAdminController {
  constructor(private readonly registry: QueueRegistry) {}

  @Get()
  async getDashboard() {
    const queues = this.registry.getRegisteredQueueNames()
      .map(name => this.registry.getQueue(name))
      .filter(q => q)
      .map(q => new BullAdapter(q!));

    const board = createBullBoard({ queues });
    return board.ui;
  }
}
```

Access at `http://localhost:3000/admin/queues`

---

## Processor Auto-Discovery

Use decorators to auto-register job handlers:

```typescript
import { Processor, QueueHandler } from 'smart-queue-nestjs';

interface OrderData {
  orderId: string;
  amount: number;
}

@Processor('orders', { concurrency: 5 })
export class OrderProcessor {
  @QueueHandler('process-order', {
    retryStrategy: {
      maxAttempts: 3,
      backoff: { type: 'exponential', delay: 1000 },
    },
  })
  async processOrder({ id, data }: { id: string; data: OrderData }) {
    console.log(`Processing order ${data.orderId}`);
    // Process order
    return { success: true };
  }

  @QueueHandler('cancel-order')
  async cancelOrder({ id, data }: { id: string; data: OrderData }) {
    console.log(`Cancelling order ${data.orderId}`);
    return { success: true };
  }
}
```

Auto-scan in your app module:

```typescript
import { Module, OnModuleInit } from '@nestjs/common';
import { NestContainer } from '@nestjs/core';
import { ProcessorScannerService, SmartQueueModule } from 'smart-queue-nestjs';

@Module({
  imports: [SmartQueueModule.forRoot({ connection: { host: 'localhost' } })],
  providers: [OrderProcessor],
})
export class AppModule implements OnModuleInit {
  constructor(
    private readonly container: NestContainer,
    private readonly scanner: ProcessorScannerService,
  ) {}

  onModuleInit() {
    const modules = Array.from(this.container.getModules().values());
    scanner.scanAndRegister(modules);
  }
}
```

---

## License

MIT