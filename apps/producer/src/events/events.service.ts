import { Injectable } from '@nestjs/common';
import { v4 as uuid } from 'uuid';
import { WorldCupEvent, NBAEvent } from '@sport-tracking/schemas';
import { KafkaService } from '../kafka/kafka.service';

@Injectable()
export class EventsService {
  constructor(private kafkaService: KafkaService) {}

  async publishWorldCupEvent(body: Omit<WorldCupEvent, 'id' | 'sport' | 'timestamp'>): Promise<string> {
    const event: WorldCupEvent = { ...body, id: uuid(), sport: 'worldcup', timestamp: Date.now() };
    await this.kafkaService.publishEvent(event);
    return event.id;
  }

  async publishNBAEvent(body: Omit<NBAEvent, 'id' | 'sport' | 'timestamp'>): Promise<string> {
    const event: NBAEvent = {
      ...body,
      id: uuid(),
      sport: 'nba',
      timestamp: Date.now(),
      gameId: body.gameId ?? body.matchId,
    };
    await this.kafkaService.publishEvent(event);
    return event.id;
  }
}
