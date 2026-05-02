import {
  Module,
  Global,
  ModuleMetadata,
  DynamicModule,
  NestModule,
  MiddlewareConsumer,
  OnModuleDestroy,
  Logger,
} from '@nestjs/common';
import { QueueRegistry } from '../core/queue-registry';
import { BullMQAdapter } from '../adapters/bullmq.adapter';
import { QueueService } from '../services/queue.service';
import { QueueMetricsService } from '../services/queue-metrics.service';
import { QueueHealthService } from '../services/queue-health.service';
import { ProcessorScannerService } from '../services/processor-scanner.service';
import {
  SmartQueueModuleOptions,
  SmartQueueModuleAsyncOptions,
  SmartQueueOptionsFactory,
  SMART_QUEUE_MODULE_OPTIONS,
} from '../interfaces/queue-options.interface';

@Global()
@Module({})
export class SmartQueueModule implements NestModule, OnModuleDestroy {
  private readonly logger = new Logger(SmartQueueModule.name);

  constructor() {
    this.logger.log('SmartQueueModule initializing...');
  }

  static forRoot(options?: SmartQueueModuleOptions): DynamicModule {
    const providers = this.createProviders(options);

    return {
      module: SmartQueueModule,
      providers,
      exports: [
        QueueRegistry,
        BullMQAdapter,
        QueueService,
        QueueMetricsService,
        QueueHealthService,
      ],
    };
  }

  static forRootAsync(options: SmartQueueModuleAsyncOptions): DynamicModule {
    const providers = this.createAsyncProviders(options);

    return {
      module: SmartQueueModule,
      imports: options.imports || [],
      providers,
      exports: [
        QueueRegistry,
        BullMQAdapter,
        QueueService,
        QueueMetricsService,
        QueueHealthService,
      ],
    };
  }

  static forFeature(queueNames: string[]): DynamicModule {
    const providers: any[] = [];

    for (const queueName of queueNames) {
      providers.push({
        provide: `QUEUE_${queueName.toUpperCase()}_SERVICE`,
        useFactory: (adapter: BullMQAdapter) => {
          adapter.registerQueue(queueName);
          return adapter;
        },
        inject: [BullMQAdapter],
      });
    }

    return {
      module: SmartQueueModule,
      providers,
      imports: [],
    };
  }

  private static createProviders(
    options?: SmartQueueModuleOptions,
  ): any[] {
    const metricsService = new QueueMetricsService();

    return [
      {
        provide: SMART_QUEUE_MODULE_OPTIONS,
        useValue: options || {},
      },
      {
        provide: QueueMetricsService,
        useValue: metricsService,
      },
      {
        provide: QueueRegistry,
        useFactory: (moduleOptions: SmartQueueModuleOptions) => {
          return new QueueRegistry(moduleOptions, metricsService);
        },
        inject: [SMART_QUEUE_MODULE_OPTIONS],
      },
      {
        provide: BullMQAdapter,
        useFactory: (registry: QueueRegistry, metrics: QueueMetricsService) => {
          if (!registry) {
            throw new Error('QueueRegistry is not available. Ensure SmartQueueModule.forRoot() is called before using queues.');
          }
          return new BullMQAdapter(registry, metrics);
        },
        inject: [QueueRegistry, QueueMetricsService],
      },
      QueueHealthService,
      ProcessorScannerService,
      {
        provide: QueueService,
        useFactory: (
          adapter: BullMQAdapter,
          healthService: QueueHealthService,
          metrics: QueueMetricsService,
          moduleOptions: SmartQueueModuleOptions,
        ) => {
          return new QueueService(adapter, healthService, metrics, moduleOptions);
        },
        inject: [BullMQAdapter, QueueHealthService, QueueMetricsService, SMART_QUEUE_MODULE_OPTIONS],
      },
    ];
  }

  private static createAsyncProviders(
    options: SmartQueueModuleAsyncOptions,
  ): any[] {
    return [
      {
        provide: SMART_QUEUE_MODULE_OPTIONS,
        useFactory: async (factory: SmartQueueOptionsFactory) => {
          return factory.createSmartQueueOptions();
        },
        inject: [options.useClass!],
      },
      {
        provide: options.useClass!,
        useClass: options.useClass!,
      },
      {
        provide: QueueMetricsService,
        useValue: new QueueMetricsService(),
      },
      {
        provide: QueueRegistry,
        useFactory: (moduleOptions: SmartQueueModuleOptions, metricsService: QueueMetricsService) => {
          return new QueueRegistry(moduleOptions, metricsService);
        },
        inject: [SMART_QUEUE_MODULE_OPTIONS, QueueMetricsService],
      },
      {
        provide: BullMQAdapter,
        useFactory: (registry: QueueRegistry, metrics: QueueMetricsService) => {
          if (!registry) {
            throw new Error('QueueRegistry is not available. Ensure SmartQueueModule.forRoot() is called before using queues.');
          }
          return new BullMQAdapter(registry, metrics);
        },
        inject: [QueueRegistry, QueueMetricsService],
      },
      QueueHealthService,
      ProcessorScannerService,
      {
        provide: QueueService,
        useFactory: (
          adapter: BullMQAdapter,
          healthService: QueueHealthService,
          metrics: QueueMetricsService,
          moduleOptions: SmartQueueModuleOptions,
        ) => {
          return new QueueService(adapter, healthService, metrics, moduleOptions);
        },
        inject: [BullMQAdapter, QueueHealthService, QueueMetricsService, SMART_QUEUE_MODULE_OPTIONS],
      },
    ];
  }

  configure(consumer: MiddlewareConsumer): void {}

  async onModuleDestroy(): Promise<void> {
    this.logger.log('SmartQueueModule destroying...');
  }
}

export interface SmartQueueModuleConfig extends ModuleMetadata {
  options?: SmartQueueModuleOptions;
}