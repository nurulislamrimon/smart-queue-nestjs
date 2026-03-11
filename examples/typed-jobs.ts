import { Injectable } from '@nestjs/common';
import { QueueService, QueueAddOptions } from '../src';

// Define job data interfaces
interface SendEmailData {
  to: string;
  subject: string;
  body: string;
}

interface ProcessPaymentData {
  orderId: string;
  amount: number;
  currency: string;
  paymentMethod: string;
}

interface GenerateReportData {
  reportType: 'daily' | 'weekly' | 'monthly';
  startDate: Date;
  endDate: Date;
  format: 'pdf' | 'excel' | 'csv';
}

@Injectable()
export class TypedJobProducer {
  constructor(private readonly queue: QueueService) {}

  async sendEmail(data: SendEmailData): Promise<void> {
    await this.queue.add<SendEmailData>('email', 'send', data);
  }

  async processPayment(data: ProcessPaymentData): Promise<void> {
    const options: QueueAddOptions<ProcessPaymentData> = {
      priority: 10,
      attempts: 5,
      backoff: {
        type: 'exponential',
        delay: 2000,
      },
    };
    
    await this.queue.add<ProcessPaymentData>('payments', 'process', data, options);
  }

  async generateReport(data: GenerateReportData): Promise<void> {
    await this.queue.add<GenerateReportData>('reports', 'generate', data);
  }

  async scheduleDailyReport(): Promise<void> {
    await this.queue.repeat<{ reportType: string }>(
      'reports',
      'daily',
      { reportType: 'daily' },
      '0 0 * * *', // Daily at midnight
    );
  }
}

// Worker with typed job handlers
interface TypedJobHandlers {
  handleSendEmail({ id, data }: { id: string; data: SendEmailData }): Promise<{ messageId: string }>;
  handleProcessPayment({ id, data }: { id: string; data: ProcessPaymentData }): Promise<{ transactionId: string }>;
  handleGenerateReport({ id, data }: { id: string; data: GenerateReportData }): Promise<{ filePath: string }>;
}

export class TypedJobProcessor implements TypedJobHandlers {
  async handleSendEmail({ id, data }: { id: string; data: SendEmailData }) {
    // TypeScript knows exact types
    console.log(`Sending email to: ${data.to}`);
    console.log(`Subject: ${data.subject}`);
    
    return { messageId: `msg-${id}` };
  }

  async handleProcessPayment({ id, data }: { id: string; data: ProcessPaymentData }) {
    // Full type safety for payment operations
    console.log(`Processing payment for order: ${data.orderId}`);
    console.log(`Amount: ${data.amount} ${data.currency}`);
    
    return { transactionId: `txn-${id}` };
  }

  async handleGenerateReport({ id, data }: { id: string; data: GenerateReportData }) {
    // Type-safe report generation
    console.log(`Generating ${data.reportType} report`);
    console.log(`Format: ${data.format}`);
    
    return { filePath: `/reports/${id}.${data.format}` };
  }
}
