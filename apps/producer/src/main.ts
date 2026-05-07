import 'reflect-metadata';
import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';

async function bootstrap(): Promise<void> {
  const app = await NestFactory.create(AppModule);
  app.enableCors();
  const port = process.env.PORT ?? 3001;
  await app.listen(port);
}

bootstrap().catch((err) => {
  console.error('[producer] fatal error', err);
  process.exit(1);
});
