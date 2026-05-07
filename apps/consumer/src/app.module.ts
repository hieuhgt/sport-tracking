import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { StateModule } from './state/state.module';
import { EventsModule } from './events/events.module';
import { KafkaModule } from './kafka/kafka.module';
import { HealthController } from './health.controller';

@Module({
  imports: [
    ConfigModule.forRoot({ isGlobal: true }),
    StateModule,
    EventsModule,
    KafkaModule,
  ],
  controllers: [HealthController],
})
export class AppModule {}
