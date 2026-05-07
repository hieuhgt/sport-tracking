// ─── Shared primitives ────────────────────────────────────────────────────────

export interface Team {
  id: string;
  name: string;
  shortName: string;
  score: number;
}

export interface Player {
  id: string;
  name: string;
  number: number;
  teamId: string;
}

// ─── World Cup event types ────────────────────────────────────────────────────

export type WorldCupEventType =
  | 'MATCH_START'
  | 'HALF_TIME'
  | 'FULL_TIME'
  | 'GOAL'
  | 'OWN_GOAL'
  | 'YELLOW_CARD'
  | 'RED_CARD'
  | 'SUBSTITUTION'
  | 'PENALTY_AWARDED'
  | 'PENALTY_SCORED'
  | 'PENALTY_MISSED';

export interface WorldCupEvent {
  id: string;
  sport: 'worldcup';
  matchId: string;
  type: WorldCupEventType;
  timestamp: number;
  minute: number;
  homeTeam: Team;
  awayTeam: Team;
  player?: Player;
  substitute?: Player;
}

// ─── NBA event types ──────────────────────────────────────────────────────────

export type NBAEventType =
  | 'GAME_START'
  | 'QUARTER_START'
  | 'QUARTER_END'
  | 'GAME_END'
  | 'BASKET_2PT'
  | 'BASKET_3PT'
  | 'FREE_THROW'
  | 'FOUL'
  | 'SUBSTITUTION'
  | 'TIMEOUT';

export interface NBAEvent {
  id: string;
  sport: 'nba';
  gameId: string;
  matchId: string;
  type: NBAEventType;
  timestamp: number;
  quarter: number;
  clock: string;
  homeTeam: Team;
  awayTeam: Team;
  player?: Player;
  substitute?: Player;
  points?: number;
}

export type SportEvent = WorldCupEvent | NBAEvent;

// ─── Match / game state (produced by consumer, consumed by frontend) ──────────

export type MatchStatus = 'UPCOMING' | 'LIVE' | 'HALF_TIME' | 'FINISHED';
export type GameStatus  = 'UPCOMING' | 'LIVE' | 'QUARTER_BREAK' | 'FINISHED';

export interface WorldCupMatchState {
  matchId: string;
  status: MatchStatus;
  minute: number;
  homeTeam: Team;
  awayTeam: Team;
  recentEvents: WorldCupEvent[];
  yellowCards: Record<string, number>;
  redCards: string[];
}

export interface NBAGameState {
  gameId: string;
  matchId: string;
  status: GameStatus;
  quarter: number;
  clock: string;
  homeTeam: Team;
  awayTeam: Team;
  recentEvents: NBAEvent[];
  fouls: Record<string, number>;
}
