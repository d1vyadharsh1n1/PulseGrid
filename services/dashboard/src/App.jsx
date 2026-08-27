import EventFeed from './components/EventFeed.jsx';
import DlqPanel from './components/DlqPanel.jsx';

function App() {
  return (
    <main className="app">
      <header className="app-header">
        <div>
          <h1>PulseGrid</h1>
          <p className="app-sub">Distributed real-time alerting pipeline</p>
        </div>
        <div className="app-badges">
          <span className="badge badge-redis">Redis Streams</span>
          <span className="badge badge-sns">SNS</span>
          <span className="badge badge-ws">WebSocket</span>
        </div>
      </header>

      <div className="dashboard-grid">
        <EventFeed />
        <DlqPanel />
      </div>
    </main>
  );
}

export default App;
