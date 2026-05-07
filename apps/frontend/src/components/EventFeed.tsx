import { WorldCupEvent, NBAEvent } from '@sport-tracking/schemas';

const WC_ICONS: Record<string, string> = {
  MATCH_START: '🟢', HALF_TIME: '⏸', FULL_TIME: '🏁',
  GOAL: '⚽', OWN_GOAL: '😬',
  YELLOW_CARD: '🟨', RED_CARD: '🟥',
  SUBSTITUTION: '🔄',
  PENALTY_AWARDED: '🎯', PENALTY_SCORED: '⚽', PENALTY_MISSED: '❌',
};

const NBA_ICONS: Record<string, string> = {
  GAME_START: '🟢', QUARTER_START: '▶️', QUARTER_END: '⏸', GAME_END: '🏁',
  BASKET_2PT: '🏀', BASKET_3PT: '🔥', FREE_THROW: '🎯',
  FOUL: '✋', SUBSTITUTION: '🔄', TIMEOUT: '⏱',
};

function wcLabel(ev: WorldCupEvent): string {
  const base = `${ev.minute}'`;
  if (ev.type === 'SUBSTITUTION') return `${base} ${ev.player?.name ?? ''} ↔ ${ev.substitute?.name ?? ''}`;
  if (ev.player) return `${base} ${ev.player.name}`;
  return base;
}

function nbaLabel(ev: NBAEvent): string {
  const base = `Q${ev.quarter} ${ev.clock}`;
  if (ev.points !== undefined && ev.player) return `${base}  ${ev.player.name} +${ev.points}`;
  if (ev.type === 'SUBSTITUTION') return `${base}  ${ev.player?.name ?? ''} ↔ ${ev.substitute?.name ?? ''}`;
  if (ev.player) return `${base}  ${ev.player.name}`;
  return base;
}

export function WorldCupEventFeed({ events }: { events: WorldCupEvent[] }) {
  return (
    <div className="event-feed">
      <h3 className="feed-title">Recent Events</h3>
      {events.length === 0 && <p className="feed-empty">Waiting for events…</p>}
      {events.map((ev) => (
        <div key={ev.id} className="feed-item">
          <span className="feed-icon">{WC_ICONS[ev.type] ?? '•'}</span>
          <span className="feed-label">{wcLabel(ev)}</span>
        </div>
      ))}
    </div>
  );
}

export function NBAEventFeed({ events }: { events: NBAEvent[] }) {
  return (
    <div className="event-feed">
      <h3 className="feed-title">Recent Events</h3>
      {events.length === 0 && <p className="feed-empty">Waiting for events…</p>}
      {events.map((ev) => (
        <div key={ev.id} className="feed-item">
          <span className="feed-icon">{NBA_ICONS[ev.type] ?? '•'}</span>
          <span className="feed-label">{nbaLabel(ev)}</span>
        </div>
      ))}
    </div>
  );
}
