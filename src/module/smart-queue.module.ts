import {
  Module,
  Global,
  ModuleMetadata,
  DynamicModule,
  NestModule,
  MiddlewareConsumer,
} from '@nestjs/common';
import { MiddlewareBuilder } from '@nestjs/core';
import { QueueRegistry } from '../core/queue-registry';
import { BullMQAdapter } from '../adapters/bullmq.adapter';
import { QueueService } from '../services/queue.service';
import {
  SmartQueueModuleOptions,
  SmartQueueModuleAsyncOptions,
  SmartQueueOptionsFactory,
  SMART_QUEUE_MODULE_OPTIONS,
} from '../interfaces/queue-options.interface';

@Global()
@Module({})
export class SmartQueueModule implements NestModule {
  constructor() {}

  static forRoot(options?: SmartQueueModuleOptions): DynamicModule {
    const providers = this.createProviders(options);
    
    return {
      module: SmartQueueModule,
      providers,
      exports: [QueueRegistry, BullMQAdapter, QueueService],
    };
  }

  static forRootAsync(options: SmartQueueModuleAsyncOptions): DynamicModule {
    const providers = this.createAsyncProviders(options);
    
    return {
      module: SmartQueueModule,
      imports: options.imports || [],
      providers,
      exports: [QueueRegistry, BullMQAdapter, QueueService],
    };
  }

  private static createProviders(options?: SmartQueueModuleOptions): any[] {
    return [
      {
        provide: SMART_QUEUE_MODULE_OPTIONS,
        useValue: options || {},
      },
      QueueRegistry,
      BullMQAdapter,
      QueueService,
    ];
  }

  private static createAsyncProviders(options: SmartQueueModuleAsyncOptions): any[] {
    const providers: Array<any> = [
      {
        provide: SMART_QUEUE_MODULE_OPTIONS,
        useFactory: (factory: SmartQueueOptionsFactory) => {
          return factory.createSmartQueueOptions();
        },
        inject: [options.useClass!],
      },
      {
        provide: options.useClass!,
        useClass: options.useClass!,
      },
      QueueRegistry,
      BullMQAdapter,
      QueueService,
    ];

    return providers;
  }

  configure(consumer: MiddlewareConsumer): void {
  }
}

export interface SmartQueueModuleConfig extends ModuleMetadata {
  options?: SmartQueueModuleOptions;
}
