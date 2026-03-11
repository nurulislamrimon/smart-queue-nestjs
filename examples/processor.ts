import { Injectable, Logger } from '@nestjs/common';
import { Processor, Process, QueueService } from '../src';

interface EmailJobData {
  to: string;
  subject: string;
  body: string;
}

interface ImageProcessingData {
  imageId: string;
  operation: 'resize' | 'compress' | 'convert';
}

@Processor('email', { concurrency: 10 })
@Injectable()
export class EmailProcessor {
  private readonly logger = new Logger(EmailProcessor.name);

  constructor(private readonly queue: QueueService) {
    this.setupEventListeners();
  }

  private setupEventListeners(): void {
    this.queue.on('email', 'completed', (jobId: string, result: unknown) => {
      this.logger.log(`Email job ${jobId} completed with result: ${JSON.stringify(result)}`);
    });

    this.queue.on('email', 'failed', (jobId: string, error: Error) => {
      this.logger.error(`Email job ${jobId} failed: ${error.message}`, error.stack);
    });

    this.queue.on('email', 'progress', (jobId: string, progress: unknown) => {
      this.logger.debug(`Email job ${jobId} progress: ${progress}`);
    });
  }

  @Process('send-email')
  async handleSendEmail({ id, data }: { id: string; data: EmailJobData }): Promise<{ messageId: string }> {
    this.logger.log(`Sending email to ${data.to} with subject: ${data.subject}`);
    
    // Simulate email sending
    await new Promise(resolve => setTimeout(resolve, 100));
    
    return { messageId: `msg-${id}-${Date.now()}` };
  }

  @Process('send-bulk-email')
  async handleBulkEmail({ id, data }: { id: string; data: { recipients: string[]; subject: string; body: string } }): Promise<{ sentCount: number }> {
    this.logger.log(`Sending bulk email to ${data.recipients.length} recipients`);
    
    // Simulate bulk email sending
    await new Promise(resolve => setTimeout(resolve, 500));
    
    return { sentCount: data.recipients.length };
  }
}

@Processor('image-processing', { concurrency: 5 })
@Injectable()
export class ImageProcessor {
  private readonly logger = new Logger(ImageProcessor.name);

  @Process('process-image')
  async handleImageProcessing({ id, data }: { id: string; data: ImageProcessingData }): Promise<{ outputPath: string }> {
    this.logger.log(`Processing image ${data.imageId} with operation: ${data.operation}`);
    
    // Simulate image processing
    await new Promise(resolve => setTimeout(resolve, 200));
    
    return { outputPath: `/processed/${data.imageId}-${data.operation}.jpg` };
  }
}
