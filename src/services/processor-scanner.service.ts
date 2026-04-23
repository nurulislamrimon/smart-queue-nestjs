import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { InstanceWrapper } from '@nestjs/core/injector/instance-wrapper';
import { Module } from '@nestjs/core/injector/module';
import { MetadataScanner } from '@nestjs/core/metadata-scanner';
import { QueueService } from './queue.service';
import {
  getProcessorMetadata,
  getQueueHandlerMetadata,
} from '../decorators/queue.decorator';
import { ProcessorMetadata } from '../decorators/queue.interface';

@Injectable()
export class ProcessorScannerService implements OnModuleInit {
  private readonly logger = new Logger(ProcessorScannerService.name);
  private readonly scannedQueues = new Set<string>();

  constructor(
    private readonly queueService: QueueService,
    private readonly metadataScanner: MetadataScanner,
  ) {}

  async onModuleInit(): Promise<void> {
    this.logger.log('ProcessorScannerService initialized');
  }

  scanAndRegister(modules: Module[]): void {
    for (const module of modules) {
      this.scanModule(module);
    }

    if (this.scannedQueues.size > 0) {
      this.logger.log(
        `Auto-registered workers for queues: ${Array.from(this.scannedQueues).join(', ')}`,
      );
    }
  }

  scanModule(module: Module): void {
    const providers = module.providers;

    for (const [token, wrapper] of providers) {
      if (this.isProcessorClass(wrapper)) {
        const instance = wrapper.instance;
        this.registerProcessorClass(instance);
      }
    }
  }

  private isProcessorClass(
    wrapper: InstanceWrapper<any>,
  ): wrapper is InstanceWrapper<any> {
    const instance = wrapper.instance;

    if (typeof instance !== 'object' || !instance.constructor) {
      return false;
    }

    const metadata = getProcessorMetadata(instance.constructor);
    return !!metadata;
  }

  private registerProcessorClass(instance: any): void {
    const queueMeta = getProcessorMetadata(instance.constructor);
    const metadata: ProcessorMetadata = queueMeta as ProcessorMetadata;

    if (!metadata?.queue) {
      return;
    }

    this.scannedQueues.add(metadata.queue);

    const prototype = Object.getPrototypeOf(instance);
    const methodNames = this.metadataScanner.getAllMethodNames(prototype);

    for (const methodName of methodNames) {
      const handlerMeta = getQueueHandlerMetadata(instance, methodName);

      if (handlerMeta) {
        const handler = instance[methodName].bind(instance);
        const queueName = handlerMeta.options?.queue || metadata.queue;
        const concurrency = handlerMeta.options?.concurrency || metadata.concurrency || 1;

        this.queueService.worker(
          queueName,
          handler,
          {
            queue: queueName,
            concurrency,
            retryStrategy: handlerMeta.options?.retryStrategy,
          },
        );

        this.logger.log(
          `Registered handler '${methodName}' for queue '${queueName}' (concurrency: ${concurrency})`,
        );
      }
    }

    if (methodNames.length === 0) {
      this.logger.warn(
        `No handlers found in processor class for queue '${metadata.queue}'`,
      );
    }
  }
}