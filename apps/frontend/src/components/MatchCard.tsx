import { Team } from '../types/events';

interface Props {
  homeTeam: Team;
  awayTeam: Team;
  statusLabel: string;
  timeLabel: string;
  statusColor: string;
}

export function MatchCard({ homeTeam, awayTeam, statusLabel, timeLabel, statusColor }: Props) {
  return (
    <div className="match-card">
      <div className="match-status" style={{ color: statusColor }}>
        {statusLabel}
      </div>

      <div className="match-scoreboard">
        <div className="team home">
          <span className="team-name">{homeTeam.shortName}</span>
          <span className="team-full">{homeTeam.name}</span>
        </div>

        <div className="scoreboard-center">
          <span className="score">{homeTeam.score}</span>
          <span className="score-sep">–</span>
          <span className="score">{awayTeam.score}</span>
          <div className="time-label">{timeLabel}</div>
        </div>

        <div className="team away">
          <span className="team-name">{awayTeam.shortName}</span>
          <span className="team-full">{awayTeam.name}</span>
        </div>
      </div>
    </div>
  );
}
