import { useEffect, useState } from 'react';

const POLL_INTERVAL_MS = 5000;

function formatReason(raw) {
  if (!raw) return '—';
  const map = {
    sns_delivery_failed: 'SNS delivery failed',
    enrichment_failed: 'Enrichment failed',
    rule_engine_error: 'Rule engine error',
    validation_failed: 'Validation failed',
    poison_pill: 'Poison pill',
  };
  return map[raw] ?? raw;
}

export default function DlqPanel() {
  const [depth, setDepth] = useState(0);
  const [recent, setRecent] = useState([]);
  const [lastCheck, setLastCheck] = useState(null);
  const [error, setError] = useState(null);

  useEffect(() => {
    let cancelled = false;
    let timer = null;

    async function tick() {
      try {
        const res = await fetch('/health/dlq');
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data = await res.json();
        if (cancelled) return;
        setDepth(Number(data?.dlq?.depth ?? 0));
        setRecent(Array.isArray(data?.dlq?.recent) ? data.dlq.recent : []);
        setLastCheck(new Date());
        setError(null);
      } catch (err) {
        if (cancelled) return;
        setError(err.message);
      }
    }

    tick();
    timer = setInterval(tick, POLL_INTERVAL_MS);

    return () => {
      cancelled = true;
      if (timer) clearInterval(timer);
    };
  }, []);

  const health = depth === 0 ? 'ok' : depth < 10 ? 'warn' : 'critical';

  return (
    <section className="panel dlq-panel">
      <header className="panel-header">
        <h2>Dead-Letter Queue</h2>
        <span className={`dlq-health dlq-${health}`}>
          {error ? '⚠ Poll failed' : health === 'ok' ? '✓ Healthy' : health === 'warn' ? '◐ Rising' : '! Critical'}
        </span>
      </header>

      <div className="dlq-stats">
        <div className={`dlq-depth depth-${health}`}>
          <span className="dlq-depth-num">{depth}</span>
          <span className="dlq-depth-label">unprocessed failure{depth === 1 ? '' : 's'}</span>
        </div>
        {lastCheck && (
          <div className="dlq-meta">
            <div>Polling every 5s</div>
            <div className="dlq-ts">Last: {lastCheck.toLocaleTimeString()}</div>
          </div>
        )}
      </div>

      {error && <div className="dlq-error">Poll error: {error}</div>}

      <div className="dlq-recent-wrap">
        <h3 className="dlq-subhead">Recent failures</h3>
        {recent.length === 0 ? (
          <div className="dlq-empty">No recent DLQ entries.</div>
        ) : (
          <ul className="dlq-recent">
            {recent.slice(0, 5).map((entry) => (
              <li key={entry.id} className="dlq-item">
                <div className="dlq-item-row">
                  <span className="tag tag-dlq">{formatReason(entry.reason)}</span>
                  <span className="dlq-item-id">{String(entry.eventId ?? entry.id ?? '').slice(-8)}</span>
                </div>
                {entry.source_id && (
                  <div className="dlq-item-row">
                    <strong>{entry.source_id}</strong>
                    {entry.event_type && <span className="feed-type">{entry.event_type}</span>}
                    {entry.value !== undefined && entry.value !== null && <span className="feed-value">{String(entry.value)}</span>}
                  </div>
                )}
                {entry.stage && <div className="dlq-stage">stage: {entry.stage}</div>}
              </li>
            ))}
          </ul>
        )}
      </div>
    </section>
  );
}
