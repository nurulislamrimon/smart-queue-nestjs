import { DynamicModule, Module, OnModuleInit, Inject, Optional } from '@nestjs/common';
import { QueueRegistry } from '../core/queue-registry';
import { Queue } from 'bullmq';

export interface BullBoardOptions {
  routePath?: string;
  apiRoute?: string;
}

@Module({})
export class BullBoardModule implements OnModuleInit {
  private readonly queueRegistry?: QueueRegistry;

  static forRoot(options?: BullBoardOptions): DynamicModule {
    return {
      module: BullBoardModule,
      providers: [
        {
          provide: 'BULL_BOARD_OPTIONS',
          useValue: options || {},
        },
        {
          provide: BullBoardModule,
          useFactory: (registry?: QueueRegistry) => {
            return new BullBoardModule(registry);
          },
          inject: [QueueRegistry],
        },
      ],
      imports: [],
      exports: [BullBoardModule],
    };
  }

  constructor(@Optional() @Inject(QueueRegistry) queueRegistry?: QueueRegistry) {
    this.queueRegistry = queueRegistry;
  }

  async onModuleInit(): Promise<void> {
    if (!this.queueRegistry) {
      throw new Error(
        'BullBoardModule requires QueueRegistry. Ensure SmartQueueModule.forRoot() is called before BullBoardModule.forRoot().',
      );
    }
  }

  static getQueues(queueRegistry?: QueueRegistry): Queue[] {
    const queues: Queue[] = [];

    if (!queueRegistry) {
      return queues;
    }

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