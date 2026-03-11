import { Injectable } from '@nestjs/common';
import { QueueService, QueueAddOptions } from '../src';

interface EmailJobData {
  to: string;
  subject: string;
  body: string;
}

interface ImageProcessingData {
  imageId: string;
  operation: 'resize' | 'compress' | 'convert';
}

@Injectable()
export class JobProducerService {
  constructor(private readonly queue: QueueService) {}

  async sendEmail(data: EmailJobData): Promise<void> {
    await this.queue.add('email', 'send-email', data);
  }

  async sendEmailWithOptions(data: EmailJobData): Promise<void> {
    const options: QueueAddOptions<EmailJobData> = {
      priority: 5,
      attempts: 3,
      backoff: {
        type: 'exponential',
        delay: 1000,
      },
      timeout: 30000,
    };
    
    await this.queue.add('email', 'send-email', data, options);
  }

  async sendDelayedEmail(data: EmailJobData, delayMs: number): Promise<void> {
    await this.queue.delay('email', 'send-email', data, delayMs);
  }

  async scheduleCronJob(data: { userId: string }): Promise<void> {
    await this.queue.repeat('notifications', 'daily-digest', data, '0 9 * * *');
  }

  async processImage(data: ImageProcessingData): Promise<void> {
    await this.queue.add('image-processing', 'process-image', data);
  }

  async removeJob(queueName: string, jobId: string): Promise<void> {
    await this.queue.remove(queueName, jobId);
  }

  async getQueueMetrics(queueName: string) {
    return this.queue.getMetrics(queueName);
  }
}
