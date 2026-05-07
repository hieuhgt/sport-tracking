import { Module } from '@nestjs/common';
import { EventsGateway } from './events.gateway';
import { StateModule } from '../state/state.module';

@Module({
  imports: [StateModule],
  providers: [EventsGateway],
  exports: [EventsGateway],
})
export class EventsModule {}
