import { useSocket } from './hooks/useSocket';
import { WorldCupDashboard } from './components/WorldCupDashboard';
import { NBADashboard } from './components/NBADashboard';

const STATUS_DOT: Record<string, string> = {
  connecting:   '#f59e0b',
  connected:    '#22c55e',
  disconnected: '#ef4444',
};

export default function App() {
  const { worldcup, nba, status } = useSocket();

  return (
    <div className="app">
      <header className="app-header">
        <h1>Live Sports Dashboard</h1>
        <div className="connection-status">
          <span
            className="status-dot"
            style={{ backgroundColor: STATUS_DOT[status] }}
          />
          <span className="status-label">{status}</span>
        </div>
      </header>

      <main className="dashboard">
        {worldcup ? (
          <WorldCupDashboard state={worldcup} />
        ) : (
          <div className="sport-panel placeholder">
            <div className="panel-header">
              <span className="sport-icon">⚽</span>
              <span className="sport-title">FIFA World Cup</span>
            </div>
            <p className="waiting">Waiting for match data…</p>
            <p className="hint">Run <code>make simulate</code> to start events</p>
          </div>
        )}

        {nba ? (
          <NBADashboard state={nba} />
        ) : (
          <div className="sport-panel placeholder">
            <div className="panel-header">
              <span className="sport-icon">🏀</span>
              <span className="sport-title">NBA</span>
            </div>
            <p className="waiting">Waiting for game data…</p>
            <p className="hint">Run <code>make simulate</code> to start events</p>
          </div>
        )}
      </main>

      <footer className="app-footer">
        <span>Powered by Kafka · Socket.io · Redis · TypeScript</span>
        <a href="http://localhost:8090" target="_blank" rel="noreferrer">
          Kafka UI →
        </a>
      </footer>
    </div>
  );
}
