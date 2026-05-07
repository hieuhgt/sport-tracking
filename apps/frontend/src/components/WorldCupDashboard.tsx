import { WorldCupMatchState } from '@sport-tracking/schemas';
import { MatchCard } from './MatchCard';
import { WorldCupEventFeed } from './EventFeed';

const STATUS_COLORS: Record<string, string> = {
  UPCOMING: '#94a3b8', LIVE: '#22c55e', HALF_TIME: '#f59e0b', FINISHED: '#64748b',
};
const STATUS_LABELS: Record<string, string> = {
  UPCOMING: 'UPCOMING', LIVE: 'LIVE', HALF_TIME: 'HALF TIME', FINISHED: 'FULL TIME',
};

export function WorldCupDashboard({ state }: { state: WorldCupMatchState }) {
  const statusColor = STATUS_COLORS[state.status] ?? '#94a3b8';
  const timeLabel   = state.status === 'LIVE' ? `${state.minute}'` : STATUS_LABELS[state.status];
  const cardStats   = Object.entries(state.yellowCards).filter(([, c]) => c > 0);

  return (
    <div className="sport-panel wc-panel">
      <div className="panel-header">
        <span className="sport-icon">⚽</span>
        <span className="sport-title">FIFA World Cup</span>
      </div>
      <MatchCard
        homeTeam={state.homeTeam} awayTeam={state.awayTeam}
        statusLabel={STATUS_LABELS[state.status] ?? state.status}
        timeLabel={timeLabel} statusColor={statusColor}
      />
      <div className="card-stats">
        {cardStats.length > 0 && (
          <div className="stat-row">
            <span className="stat-icon">🟨</span>
            {cardStats.map(([pid, count]) => (
              <span key={pid} className="stat-badge">{pid.slice(-4)} {count > 1 ? `×${count}` : ''}</span>
            ))}
          </div>
        )}
        {state.redCards.length > 0 && (
          <div className="stat-row">
            <span className="stat-icon">🟥</span>
            {state.redCards.map((pid) => (
              <span key={pid} className="stat-badge red">{pid.slice(-4)}</span>
            ))}
          </div>
        )}
      </div>
      <WorldCupEventFeed events={state.recentEvents} />
    </div>
  );
}
