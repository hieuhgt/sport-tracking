import { Injectable, Logger } from '@nestjs/common';
import { v4 as uuid } from 'uuid';
import {
  WorldCupEvent, WorldCupEventType,
  NBAEvent, NBAEventType,
  Team, Player,
} from '@sport-tracking/schemas';
import { KafkaService } from '../kafka/kafka.service';

const WC_MATCH_ID = 'wc-match-1';
const NBA_GAME_ID = 'nba-game-1';

const brazilPlayers: Player[] = [
  { id: 'p1', name: 'Alisson',     number: 1,  teamId: 'brazil' },
  { id: 'p2', name: 'Danilo',      number: 2,  teamId: 'brazil' },
  { id: 'p3', name: 'Marquinhos',  number: 4,  teamId: 'brazil' },
  { id: 'p4', name: 'Casemiro',    number: 5,  teamId: 'brazil' },
  { id: 'p5', name: 'Vinicius Jr', number: 7,  teamId: 'brazil' },
  { id: 'p6', name: 'Neymar Jr',   number: 10, teamId: 'brazil' },
  { id: 'p7', name: 'Rodrygo',     number: 11, teamId: 'brazil' },
];

const germanyPlayers: Player[] = [
  { id: 'p8',  name: 'Neuer',   number: 1,  teamId: 'germany' },
  { id: 'p9',  name: 'Kroos',   number: 8,  teamId: 'germany' },
  { id: 'p10', name: 'Müller',  number: 13, teamId: 'germany' },
  { id: 'p11', name: 'Havertz', number: 7,  teamId: 'germany' },
  { id: 'p12', name: 'Gnabry',  number: 10, teamId: 'germany' },
  { id: 'p13', name: 'Kimmich', number: 6,  teamId: 'germany' },
];

const lakersPlayers: Player[] = [
  { id: 'n1', name: 'LeBron James',     number: 23, teamId: 'lakers' },
  { id: 'n2', name: 'Anthony Davis',    number: 3,  teamId: 'lakers' },
  { id: 'n3', name: 'Austin Reaves',    number: 15, teamId: 'lakers' },
  { id: 'n4', name: "D'Angelo Russell", number: 1,  teamId: 'lakers' },
];

const warriorsPlayers: Player[] = [
  { id: 'n5', name: 'Stephen Curry',  number: 30, teamId: 'warriors' },
  { id: 'n6', name: 'Klay Thompson',  number: 11, teamId: 'warriors' },
  { id: 'n7', name: 'Draymond Green', number: 23, teamId: 'warriors' },
  { id: 'n8', name: 'Andrew Wiggins', number: 22, teamId: 'warriors' },
];

@Injectable()
export class SimulatorService {
  private readonly logger = new Logger(SimulatorService.name);

  private wcTimer: NodeJS.Timeout | null = null;
  private nbaTimer: NodeJS.Timeout | null = null;

  private wcState = {
    minute: 0,
    homeTeam: { id: 'brazil',  name: 'Brazil',  shortName: 'BRA', score: 0 } as Team,
    awayTeam: { id: 'germany', name: 'Germany', shortName: 'GER', score: 0 } as Team,
    started: false,
    finished: false,
    yellowCards: {} as Record<string, number>,
  };

  private nbaState = {
    quarter: 1,
    secondsLeft: 720,
    homeTeam: { id: 'lakers',   name: 'Los Angeles Lakers',    shortName: 'LAL', score: 0 } as Team,
    awayTeam: { id: 'warriors', name: 'Golden State Warriors', shortName: 'GSW', score: 0 } as Team,
    started: false,
    finished: false,
  };

  constructor(private kafkaService: KafkaService) {}

  start(): void {
    if (this.wcTimer || this.nbaTimer) return;
    this.logger.log('Starting simulator');
    this.wcTimer  = setInterval(
      async () => { try { await this.wcTick();  } catch (e) { this.logger.error('WC tick error', e);  } },
      this.rand(2000, 4000),
    );
    this.nbaTimer = setInterval(
      async () => { try { await this.nbaTick(); } catch (e) { this.logger.error('NBA tick error', e); } },
      this.rand(1500, 3000),
    );
  }

  stop(): void {
    if (this.wcTimer)  { clearInterval(this.wcTimer);  this.wcTimer  = null; }
    if (this.nbaTimer) { clearInterval(this.nbaTimer); this.nbaTimer = null; }
    this.logger.log('Simulator stopped');
  }

  isRunning(): boolean {
    return this.wcTimer !== null || this.nbaTimer !== null;
  }

  private rand(min: number, max: number): number {
    return Math.floor(Math.random() * (max - min + 1)) + min;
  }

  private pick<T>(arr: T[]): T {
    return arr[Math.floor(Math.random() * arr.length)];
  }

  private clockStr(s: number): string {
    return `${String(Math.floor(s / 60)).padStart(2, '0')}:${String(s % 60).padStart(2, '0')}`;
  }

  private async wcTick(): Promise<void> {
    if (this.wcState.finished) return;

    if (!this.wcState.started) {
      this.wcState.started = true;
      await this.kafkaService.publishEvent({
        id: uuid(), sport: 'worldcup', matchId: WC_MATCH_ID,
        type: 'MATCH_START', timestamp: Date.now(), minute: 0,
        homeTeam: { ...this.wcState.homeTeam }, awayTeam: { ...this.wcState.awayTeam },
      });
      return;
    }

    this.wcState.minute += this.rand(1, 4);

    if (this.wcState.minute >= 45 && this.wcState.minute < 50) {
      this.wcState.minute = 45;
      await this.kafkaService.publishEvent({
        id: uuid(), sport: 'worldcup', matchId: WC_MATCH_ID,
        type: 'HALF_TIME', timestamp: Date.now(), minute: 45,
        homeTeam: { ...this.wcState.homeTeam }, awayTeam: { ...this.wcState.awayTeam },
      });
      this.wcState.minute = 46;
      return;
    }

    if (this.wcState.minute >= 90) {
      this.wcState.finished = true;
      await this.kafkaService.publishEvent({
        id: uuid(), sport: 'worldcup', matchId: WC_MATCH_ID,
        type: 'FULL_TIME', timestamp: Date.now(), minute: 90,
        homeTeam: { ...this.wcState.homeTeam }, awayTeam: { ...this.wcState.awayTeam },
      });
      return;
    }

    const roll = Math.random();
    let type: WorldCupEventType;
    let player: Player | undefined;
    let substitute: Player | undefined;

    if (roll < 0.25) {
      const isHome    = Math.random() < 0.55;
      const isOwnGoal = Math.random() < 0.08;
      player = isHome ? this.pick(brazilPlayers) : this.pick(germanyPlayers);
      if (isOwnGoal) {
        type = 'OWN_GOAL';
        if (isHome) this.wcState.awayTeam.score++; else this.wcState.homeTeam.score++;
      } else {
        type = 'GOAL';
        if (isHome) this.wcState.homeTeam.score++; else this.wcState.awayTeam.score++;
      }
    } else if (roll < 0.42) {
      player = Math.random() < 0.5 ? this.pick(brazilPlayers) : this.pick(germanyPlayers);
      this.wcState.yellowCards[player.id] = (this.wcState.yellowCards[player.id] ?? 0) + 1;
      type = this.wcState.yellowCards[player.id] >= 2 ? 'RED_CARD' : 'YELLOW_CARD';
    } else if (roll < 0.55) {
      const isHome = Math.random() < 0.5;
      type      = 'SUBSTITUTION';
      player    = isHome ? this.pick(brazilPlayers) : this.pick(germanyPlayers);
      substitute = isHome ? this.pick(brazilPlayers) : this.pick(germanyPlayers);
    } else {
      return;
    }

    await this.kafkaService.publishEvent({
      id: uuid(), sport: 'worldcup', matchId: WC_MATCH_ID,
      type, timestamp: Date.now(), minute: this.wcState.minute,
      homeTeam: { ...this.wcState.homeTeam }, awayTeam: { ...this.wcState.awayTeam },
      player, substitute,
    });

    this.logger.debug(`WC ${type} min=${this.wcState.minute}`);
  }

  private async nbaTick(): Promise<void> {
    if (this.nbaState.finished) return;

    if (!this.nbaState.started) {
      this.nbaState.started = true;
      await this.kafkaService.publishEvent({
        id: uuid(), sport: 'nba', gameId: NBA_GAME_ID, matchId: NBA_GAME_ID,
        type: 'GAME_START', timestamp: Date.now(), quarter: 1, clock: '12:00',
        homeTeam: { ...this.nbaState.homeTeam }, awayTeam: { ...this.nbaState.awayTeam },
      });
      return;
    }

    this.nbaState.secondsLeft -= this.rand(5, 25);

    if (this.nbaState.secondsLeft <= 0) {
      if (this.nbaState.quarter >= 4) {
        this.nbaState.finished = true;
        await this.kafkaService.publishEvent({
          id: uuid(), sport: 'nba', gameId: NBA_GAME_ID, matchId: NBA_GAME_ID,
          type: 'GAME_END', timestamp: Date.now(), quarter: 4, clock: '00:00',
          homeTeam: { ...this.nbaState.homeTeam }, awayTeam: { ...this.nbaState.awayTeam },
        });
      } else {
        await this.kafkaService.publishEvent({
          id: uuid(), sport: 'nba', gameId: NBA_GAME_ID, matchId: NBA_GAME_ID,
          type: 'QUARTER_END', timestamp: Date.now(), quarter: this.nbaState.quarter, clock: '00:00',
          homeTeam: { ...this.nbaState.homeTeam }, awayTeam: { ...this.nbaState.awayTeam },
        });
        this.nbaState.quarter++;
        this.nbaState.secondsLeft = 720;
      }
      return;
    }

    const roll   = Math.random();
    const isHome = Math.random() < 0.5;
    let type: NBAEventType;
    let player: Player | undefined;
    let substitute: Player | undefined;
    let points: number | undefined;

    if (roll < 0.40) {
      type = 'BASKET_2PT'; points = 2;
      player = isHome ? this.pick(lakersPlayers) : this.pick(warriorsPlayers);
      if (isHome) this.nbaState.homeTeam.score += 2; else this.nbaState.awayTeam.score += 2;
    } else if (roll < 0.60) {
      type = 'BASKET_3PT'; points = 3;
      player = isHome ? this.pick(lakersPlayers) : this.pick(warriorsPlayers);
      if (isHome) this.nbaState.homeTeam.score += 3; else this.nbaState.awayTeam.score += 3;
    } else if (roll < 0.70) {
      type = 'FREE_THROW'; points = 1;
      player = isHome ? this.pick(lakersPlayers) : this.pick(warriorsPlayers);
      if (isHome) this.nbaState.homeTeam.score += 1; else this.nbaState.awayTeam.score += 1;
    } else if (roll < 0.80) {
      type = 'FOUL';
      player = isHome ? this.pick(lakersPlayers) : this.pick(warriorsPlayers);
    } else if (roll < 0.88) {
      type      = 'SUBSTITUTION';
      player    = isHome ? this.pick(lakersPlayers) : this.pick(warriorsPlayers);
      substitute = isHome ? this.pick(lakersPlayers) : this.pick(warriorsPlayers);
    } else if (roll < 0.93) {
      type = 'TIMEOUT';
    } else {
      return;
    }

    await this.kafkaService.publishEvent({
      id: uuid(), sport: 'nba', gameId: NBA_GAME_ID, matchId: NBA_GAME_ID,
      type, timestamp: Date.now(),
      quarter: this.nbaState.quarter, clock: this.clockStr(this.nbaState.secondsLeft),
      homeTeam: { ...this.nbaState.homeTeam }, awayTeam: { ...this.nbaState.awayTeam },
      player, substitute, points,
    });

    this.logger.debug(`NBA ${type} Q${this.nbaState.quarter} ${this.clockStr(this.nbaState.secondsLeft)}`);
  }
}
