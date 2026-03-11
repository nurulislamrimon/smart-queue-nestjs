import { SetMetadata } from '@nestjs/common';

export const QUEUE_METADATA_KEY = 'smart-queue:queue';
export const PROCESSOR_METADATA_KEY = 'smart-queue:processor';
export const PROCESS_HANDLER_METADATA_KEY = 'smart-queue:process';

export interface QueueMetadata {
  name: string;
}

export interface ProcessorMetadata {
  queue: string;
  concurrency?: number;
}

export interface ProcessHandlerMetadata {
  name: string;
  options?: {
    concurrency?: number;
  };
}

export function Queue(name: string): ClassDecorator {
  return (target: object) => {
    SetMetadata(QUEUE_METADATA_KEY, { name })(target as Function);
  };
}

export function Processor(
  queue: string,
  options?: { concurrency?: number },
): ClassDecorator {
  return (target: object) => {
    SetMetadata(PROCESSOR_METADATA_KEY, { queue, concurrency: options?.concurrency })(target as Function);
  };
}

export function Process(
  name: string,
  options?: { concurrency?: number },
): MethodDecorator {
  return (target: object, propertyKey: string | symbol, descriptor: PropertyDescriptor) => {
    SetMetadata(PROCESS_HANDLER_METADATA_KEY, { 
      name, 
      options 
    })(target, propertyKey, descriptor);
    return descriptor;
  };
}

export function getQueueMetadata(target: Function): QueueMetadata | undefined {
  return Reflect.getMetadata(QUEUE_METADATA_KEY, target);
}

export function getProcessorMetadata(target: Function): ProcessorMetadata | undefined {
  return Reflect.getMetadata(PROCESSOR_METADATA_KEY, target);
}

export function getProcessHandlerMetadata(
  target: object,
  propertyKey: string | symbol,
): ProcessHandlerMetadata | undefined {
  return Reflect.getMetadata(PROCESS_HANDLER_METADATA_KEY, target, propertyKey);
}
