import { NestFactory } from '@nestjs/core';
import { Module, Controller, Get } from '@nestjs/common';
import { createBullBoard } from '@bull-board/api';
import { BullAdapter } from '@bull-board/api';
import { AppModule } from './basic/app.module';
import { QueueRegistry } from '../src/core/queue-registry';

@Controller('admin/queues')
export class QueueAdminController {
  constructor(private readonly registry: QueueRegistry) {}

  @Get()
  async mountDashboard() {
    const queues: any[] = [];
    const registeredQueueNames = this.registry.getRegisteredQueueNames();

    for (const name of registeredQueueNames) {
      const queue = this.registry.getQueue(name);
      if (queue) {
        queues.push(new BullAdapter(queue));
      }
    }

    const board = createBullBoard({
      queues,
    });

    return board.ui;
  }
}

async function bootstrap() {
  const app = await NestFactory.create(AppModule);

  app.use('/admin/queues', (req, res, next) => {
    if (req.path.startsWith('/api')) {
      return res.redirect('/admin/queues/api/queues');
    }
    next();
  });

  await app.listen(3000);
}

bootstrap();