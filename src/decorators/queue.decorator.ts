import { SetMetadata } from '@nestjs/common';
import {
  QueueHandlerMetadata,
  ProcessorMetadata,
  QueueMetadata,
} from './queue.interface';

export const QUEUE_METADATA_KEY = 'smart-queue:queue';
export const PROCESSOR_METADATA_KEY = 'smart-queue:processor';
export const QUEUE_HANDLER_METADATA_KEY = 'smart-queue:handler';

export function Queue(name: string): ClassDecorator {
  return SetMetadata(QUEUE_METADATA_KEY, { name });
}

export function Processor(
  queue: string,
  options?: { concurrency?: number },
): ClassDecorator {
  return SetMetadata(PROCESSOR_METADATA_KEY, { queue, concurrency: options?.concurrency });
}

export function QueueHandler(
  name: string,
  options?: {
    queue?: string;
    concurrency?: number;
    retryStrategy?: {
      maxAttempts: number;
      backoff?: { type: 'exponential' | 'fixed'; delay: number };
    };
    deadLetterQueue?: { enabled: boolean; maxRetries?: number; queueName?: string };
  },
): MethodDecorator {
  return SetMetadata(QUEUE_HANDLER_METADATA_KEY, { name, options });
}

export function getQueueMetadata(target: Function): QueueMetadata | undefined {
  return Reflect.getMetadata(QUEUE_METADATA_KEY, target);
}

export function getProcessorMetadata(
  target: Function,
): ProcessorMetadata | undefined {
  return Reflect.getMetadata(PROCESSOR_METADATA_KEY, target);
}

export function getQueueHandlerMetadata(
  target: object,
  propertyKey: string | symbol,
): QueueHandlerMetadata | undefined {
  return Reflect.getMetadata(QUEUE_HANDLER_METADATA_KEY, target, propertyKey);
}