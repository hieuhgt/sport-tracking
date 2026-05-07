import { Injectable } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import Redis from 'ioredis';
import {
  SportEvent, WorldCupEvent, NBAEvent,
  WorldCupMatchState, NBAGameState,
} from '@sport-tracking/schemas';

const MAX_RECENT = 20;

@Injectable()
export class StateService {
  private redis: Redis;

  constructor(config: ConfigService) {
    this.redis = new Redis(config.get<string>('REDIS_URL') ?? 'redis://localhost:6379');
  }

  async applyWorldCupEvent(event: WorldCupEvent): Promise<WorldCupMatchState> {
    const key = `wc:match:${event.matchId}`;
    const raw = await this.redis.get(key);

    const state: WorldCupMatchState = raw
      ? JSON.parse(raw)
      : {
          matchId: event.matchId,
          status: 'UPCOMING',
          minute: 0,
          homeTeam: { ...event.homeTeam },
          awayTeam: { ...event.awayTeam },
          recentEvents: [],
          yellowCards: {},
          redCards: [],
        };

    state.homeTeam = { ...event.homeTeam };
    state.awayTeam = { ...event.awayTeam };
    state.minute   = event.minute;

    switch (event.type) {
      case 'MATCH_START': state.status = 'LIVE';      break;
      case 'HALF_TIME':   state.status = 'HALF_TIME'; break;
      case 'FULL_TIME':   state.status = 'FINISHED';  break;
      case 'YELLOW_CARD':
        if (event.player) {
          state.yellowCards[event.player.id] = (state.yellowCards[event.player.id] ?? 0) + 1;
        }
        break;
      case 'RED_CARD':
        if (event.player && !state.redCards.includes(event.player.id)) {
          state.redCards.push(event.player.id);
        }
        break;
    }

    state.recentEvents = [event, ...state.recentEvents].slice(0, MAX_RECENT);
    await this.redis.set(key, JSON.stringify(state), 'EX', 86400);
    return state;
  }

  async applyNBAEvent(event: NBAEvent): Promise<NBAGameState> {
    const key = `nba:game:${event.gameId}`;
    const raw = await this.redis.get(key);

    const state: NBAGameState = raw
      ? JSON.parse(raw)
      : {
          gameId: event.gameId,
          matchId: event.matchId,
          status: 'UPCOMING',
          quarter: 1,
          clock: '12:00',
          homeTeam: { ...event.homeTeam },
          awayTeam: { ...event.awayTeam },
          recentEvents: [],
          fouls: {},
        };

    state.homeTeam = { ...event.homeTeam };
    state.awayTeam = { ...event.awayTeam };
    state.quarter  = event.quarter;
    state.clock    = event.clock;

    switch (event.type) {
      case 'GAME_START':    state.status = 'LIVE';          break;
      case 'QUARTER_START': state.status = 'LIVE';          break;
      case 'QUARTER_END':   state.status = 'QUARTER_BREAK'; break;
      case 'GAME_END':      state.status = 'FINISHED';      break;
      case 'FOUL':
        if (event.player) {
          state.fouls[event.player.id] = (state.fouls[event.player.id] ?? 0) + 1;
        }
        break;
    }

    state.recentEvents = [event, ...state.recentEvents].slice(0, MAX_RECENT);
    await this.redis.set(key, JSON.stringify(state), 'EX', 86400);
    return state;
  }

  async applyEvent(event: SportEvent): Promise<WorldCupMatchState | NBAGameState> {
    return event.sport === 'worldcup'
      ? this.applyWorldCupEvent(event as WorldCupEvent)
      : this.applyNBAEvent(event as NBAEvent);
  }

  async getWorldCupState(matchId: string): Promise<WorldCupMatchState | null> {
    const raw = await this.redis.get(`wc:match:${matchId}`);
    return raw ? JSON.parse(raw) : null;
  }

  async getNBAState(gameId: string): Promise<NBAGameState | null> {
    const raw = await this.redis.get(`nba:game:${gameId}`);
    return raw ? JSON.parse(raw) : null;
  }
}
