export interface QueueMetadata {
  name: string;
}

export interface ProcessorMetadata {
  queue: string;
  concurrency?: number;
}

export interface QueueHandlerMetadata {
  name: string;
  options?: {
    queue?: string;
    concurrency?: number;
    retryStrategy?: {
      maxAttempts: number;
      backoff?: { type: 'exponential' | 'fixed'; delay: number };
    };
    deadLetterQueue?: { enabled: boolean; maxRetries?: number; queueName?: string };
  };
}