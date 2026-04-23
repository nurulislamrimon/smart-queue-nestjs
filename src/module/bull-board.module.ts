import { DynamicModule, Module, OnModuleInit } from '@nestjs/common';
import { QueueRegistry } from '../core/queue-registry';

export interface BullBoardOptions {
  routePath?: string;
  apiRoute?: string;
}

@Module({})
export class BullBoardModule implements OnModuleInit {
  private queueRegistry: QueueRegistry;

  static forRoot(options?: BullBoardOptions): DynamicModule {
    return {
      module: BullBoardModule,
      providers: [
        {
          provide: 'BULL_BOARD_OPTIONS',
          useValue: options || {},
        },
      ],
      imports: [],
      exports: [],
    };
  }

  constructor(queueRegistry: QueueRegistry) {
    this.queueRegistry = queueRegistry;
  }

  async onModuleInit(): Promise<void> {}

  static getQueues(queueRegistry: QueueRegistry): any[] {
    const queues: any[] = [];
    const registeredQueueNames = queueRegistry.getRegisteredQueueNames();

    for (const name of registeredQueueNames) {
      const queue = queueRegistry.getQueue(name);
      if (queue) {
        queues.push(queue);
      }
    }

    return queues;
  }
}