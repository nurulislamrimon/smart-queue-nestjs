# smart-queue-nestjs

A powerful yet simple BullMQ integration for NestJS applications. Simplify queue management while exposing full BullMQ capabilities.

## Features

- **Queue Decorator** - Register queues automatically with `@Queue("name")`
- **Processor Decorator** - Define worker classes with `@Processor("name")`
- **Process Decorator** - Handle specific job types with `@Process("handler")`
- **Queue Service** - Injectable service for producing jobs
- **Typed Jobs** - Full TypeScript generics support
- **Retry Strategies** - Easy retry configuration
- **Rate Limiting** - Built-in rate limiting support
- **Job Events** - Listen for completed, failed, progress events
- **Queue Metrics** - Expose waiting, active, completed, failed, delayed stats
- **Error Handling** - Worker failures won't crash your application

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
    }),
  ],
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
export class EmailService {
  constructor(private readonly queue: QueueService) {}

  async sendEmail(data: EmailJobData) {
    await this.queue.add('email', 'send-email', data);
  }
}
```

### 3. Create a Worker

```typescript
import { Processor, Process, QueueService } from 'smart-queue-nestjs';
import { Injectable, Logger } from '@nestjs/common';

interface EmailJobData {
  to: string;
  subject: string;
  body: string;
}

@Processor('email', { concurrency: 5 })
@Injectable()
export class EmailProcessor {
  private readonly logger = new Logger(EmailProcessor.name);

  constructor(private readonly queue: QueueService) {
    // Register event listeners
    this.queue.on('email', 'completed', (jobId, result) => {
      this.logger.log(`Email job ${jobId} completed: ${result}`);
    });

    this.queue.on('email', 'failed', (jobId, error) => {
      this.logger.error(`Email job ${jobId} failed: ${error.message}`);
    });
  }

  @Process('send-email')
  async handleSendEmail({ id, data }: { id: string; data: EmailJobData }) {
    this.logger.log(`Sending email to ${data.to}`);
    // Your email sending logic here
    return { success: true, messageId: `msg-${id}` };
  }
}
```

## Advanced Usage

### Async Configuration

```typescript
import { Module } from '@nestjs/common';
import { SmartQueueModule } from 'smart-queue-nestjs';
import { ConfigService } from './config.service';

@Module({
  imports: [
    SmartQueueModule.forRootAsync({
      useFactory: (config: ConfigService) => ({
        connection: {
          host: config.get('REDIS_HOST'),
          port: config.get('REDIS_PORT'),
        },
        defaultJobOptions: {
          attempts: 3,
          backoff: {
            type: 'exponential',
            delay: 1000,
          },
        },
      }),
      inject: [ConfigService],
    }),
  ],
})
export class AppModule {}
```

### Delayed Jobs

```typescript
await this.queue.delay('email', 'send-email', data, 5000); // 5 seconds delay
```

### Repeating Jobs (Cron)

```typescript
await this.queue.repeat(
  'notifications',
  'daily-digest',
  { userId: '123' },
  '0 9 * * *', // Every day at 9 AM
);
```

### Rate Limiting

```typescript
@Processor('rate-limited-queue', {
  limiter: {
    max: 10,      // Max 10 jobs
    duration: 1000, // Per 1 second
  },
})
@Injectable()
export class RateLimitedProcessor {}
```

### Retry Strategy

```typescript
this.queue.worker(
  'my-queue',
  async ({ id, data }) => {
    // Your job logic
  },
  {
    retryStrategy: {
      attempts: 5,
      backoff: {
        type: 'exponential',
        delay: 1000,
      },
    },
  },
);
```

### Queue Metrics

```typescript
const metrics = await this.queue.getMetrics('email');
console.log(metrics);
// {
//   waiting: 10,
//   active: 2,
//   completed: 100,
//   failed: 5,
//   delayed: 3,
//   paused: 0
// }
```

### Remove Jobs

```typescript
await this.queue.remove('email', 'job-id-123');
```

## TypeScript Generics

The library supports full TypeScript generics for type-safe job payloads:

```typescript
interface UserNotification {
  userId: string;
  message: string;
}

// Producer
await this.queue.add<UserNotification>('notifications', 'push', {
  userId: '123',
  message: 'Hello!',
});

// Worker
@Process('push')
async handlePushNotification({ 
  id, 
  data 
}: { 
  id: string; 
  data: UserNotification 
}) {
  // TypeScript knows data.userId and data.message
}
```

## Job Options

All BullMQ job options are supported:

```typescript
await this.queue.add('queue-name', 'job-name', data, {
  jobId: 'unique-id',
  priority: 10,
  delay: 1000,
  attempts: 3,
  backoff: { type: 'exponential', delay: 1000 },
  timeout: 30000,
  removeOnComplete: true,
  removeOnFail: 100,
});
```

## Error Handling

Worker failures are handled gracefully and won't crash your application:

```typescript
@Processor('safe-queue')
@Injectable()
export class SafeProcessor {
  @Process('risky-operation')
  async handleRiskyOperation({ id, data }: { id: string; data: any }) {
    try {
      // Risky operation
    } catch (error) {
      // Error is logged, job is marked as failed
      // Application keeps running
      throw error; // Optionally rethrow for retry logic
    }
  }
}
```

## API Reference

### QueueService Methods

| Method | Description |
|--------|-------------|
| `add(queue, name, data, options)` | Add a new job to the queue |
| `delay(queue, name, data, delay)` | Add a delayed job |
| `repeat(queue, name, data, cron)` | Add a repeating job (cron) |
| `remove(queue, jobId)` | Remove a job by ID |
| `getJob(queue, jobId)` | Get a job by ID |
| `getMetrics(queue)` | Get queue statistics |
| `pause(queue)` | Pause the queue |
| `resume(queue)` | Resume the queue |
| `drain(queue)` | Drain all waiting jobs |
| `clean(queue, grace, status)` | Clean completed/failed jobs |
| `worker(queue, processor, options)` | Create a worker |
| `on(queue, event, handler)` | Register event listener |

## License

MIT
