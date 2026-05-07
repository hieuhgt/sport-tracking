import { NBAGameState } from '@sport-tracking/schemas';
import { MatchCard } from './MatchCard';
import { NBAEventFeed } from './EventFeed';

const STATUS_COLORS: Record<string, string> = {
  UPCOMING: '#94a3b8', LIVE: '#f97316', QUARTER_BREAK: '#f59e0b', FINISHED: '#64748b',
};
const STATUS_LABELS: Record<string, string> = {
  UPCOMING: 'UPCOMING', LIVE: 'LIVE', QUARTER_BREAK: 'BREAK', FINISHED: 'FINAL',
};

export function NBADashboard({ state }: { state: NBAGameState }) {
  const statusColor = STATUS_COLORS[state.status] ?? '#94a3b8';
  const timeLabel   = `Q${state.quarter} ${state.clock}`;
  const foulLeaders = Object.entries(state.fouls).sort(([, a], [, b]) => b - a).slice(0, 3);

  return (
    <div className="sport-panel nba-panel">
      <div className="panel-header">
        <span className="sport-icon">🏀</span>
        <span className="sport-title">NBA</span>
      </div>
      <MatchCard
        homeTeam={state.homeTeam} awayTeam={state.awayTeam}
        statusLabel={STATUS_LABELS[state.status] ?? state.status}
        timeLabel={timeLabel} statusColor={statusColor}
      />
      {foulLeaders.length > 0 && (
        <div className="card-stats">
          <div className="stat-row">
            <span className="stat-icon">✋</span>
            <span className="stat-label">Fouls:</span>
            {foulLeaders.map(([pid, count]) => (
              <span key={pid} className="stat-badge">{pid.slice(-4)} ({count})</span>
            ))}
          </div>
        </div>
      )}
      <NBAEventFeed events={state.recentEvents} />
    </div>
  );
}
