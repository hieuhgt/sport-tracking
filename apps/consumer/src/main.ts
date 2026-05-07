import 'reflect-metadata';
import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';
import { RedisIoAdapter } from './adapters/redis-io.adapter';

async function bootstrap(): Promise<void> {
  const app = await NestFactory.create(AppModule);
  app.enableCors();

  const redisIoAdapter = new RedisIoAdapter(app);
  await redisIoAdapter.connectToRedis(process.env.REDIS_URL ?? 'redis://localhost:6379');
  app.useWebSocketAdapter(redisIoAdapter);

  const port = process.env.PORT ?? 3002;
  await app.listen(port);
}

bootstrap().catch((err) => {
  console.error('[consumer] fatal error', err);
  process.exit(1);
});
