import { Module } from '@nestjs/common';
import { KafkaConsumerService } from './kafka-consumer.service';
import { StateModule } from '../state/state.module';
import { EventsModule } from '../events/events.module';

@Module({
  imports: [StateModule, EventsModule],
  providers: [KafkaConsumerService],
})
export class KafkaModule {}
