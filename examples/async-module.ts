import { Module } from '@nestjs/common';
import { SmartQueueModule } from '../src';
import { ConfigService } from './config.service';
import { QueueModuleOptions } from './queue-options.interface';

@Module({
  imports: [
    SmartQueueModule.forRootAsync({
      useFactory: (config: ConfigService): QueueModuleOptions => ({
        connection: {
          host: config.get('REDIS_HOST', 'localhost'),
          port: config.get('REDIS_PORT', 6379),
          password: config.get('REDIS_PASSWORD'),
          db: config.get('REDIS_DB', 0),
          keyPrefix: config.get('QUEUE_PREFIX', 'my-app'),
        },
        defaultJobOptions: {
          attempts: config.get('JOB_ATTEMPTS', 3),
          backoff: {
            type: 'exponential',
            delay: config.get('JOB_BACKOFF_DELAY', 1000),
          },
          removeOnComplete: config.get('JOB_REMOVE_ON_COMPLETE', true),
          removeOnFail: config.get('JOB_REMOVE_ON_FAIL', 100),
        },
        defaultWorkerOptions: {
          concurrency: config.get('WORKER_CONCURRENCY', 10),
        },
      }),
      inject: [ConfigService],
    }),
  ],
})
export class AppModule {}
