import { Body, Controller, Get, HttpCode, Post, BadRequestException } from '@nestjs/common';
import { EventsService } from './events.service';

@Controller()
export class EventsController {
  constructor(private eventsService: EventsService) {}

  @Get('health')
  health() {
    return { ok: true, timestamp: new Date().toISOString() };
  }

  @Post('events/worldcup')
  @HttpCode(201)
  async publishWorldCup(@Body() body: any) {
    if (!body.matchId || !body.type || body.minute === undefined) {
      throw new BadRequestException('matchId, type and minute are required');
    }
    const eventId = await this.eventsService.publishWorldCupEvent(body);
    return { ok: true, eventId };
  }

  @Post('events/nba')
  @HttpCode(201)
  async publishNba(@Body() body: any) {
    if (!body.matchId || !body.type || !body.quarter || !body.clock) {
      throw new BadRequestException('matchId, type, quarter and clock are required');
    }
    const eventId = await this.eventsService.publishNBAEvent(body);
    return { ok: true, eventId };
  }
}
