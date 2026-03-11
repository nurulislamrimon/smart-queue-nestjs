import { Module } from '@nestjs/common';
import { SmartQueueModule, QueueService } from '../src';

@Module({
  imports: [
    SmartQueueModule.forRoot({
      connection: {
        host: 'localhost',
        port: 6379,
        password: 'optional-password',
      },
      defaultJobOptions: {
        attempts: 3,
        backoff: {
          type: 'exponential',
          delay: 1000,
        },
        removeOnComplete: true,
        removeOnFail: 100,
      },
    }),
  ],
  providers: [QueueService],
  exports: [QueueService],
})
export class AppModule {}
