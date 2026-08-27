import { useEffect, useRef, useState } from 'react';

const MAX_ITEMS = 50;
const WS_URL = `${window.location.protocol === 'https:' ? 'wss:' : 'ws:'}//${window.location.host}/ws`;

function dispositionTag(disposition) {
  switch (disposition) {
    case 'processed_alert_sent':
      return { label: 'ALERT SENT', cls: 'tag tag-alert' };
    case 'processed_alert_skipped':
      return { label: 'ALERT SKIPPED', cls: 'tag tag-alert-skip' };
    case 'processed_no_alert':
      return { label: 'OK', cls: 'tag tag-ok' };
    case 'rate_limited':
      return { label: 'RATE LIMIT', cls: 'tag tag-rate' };
    case 'dedup_skipped':
      return { label: 'DEDUP', cls: 'tag tag-dedup' };
    case 'dlq':
      return { label: 'DLQ', cls: 'tag tag-dlq' };
    default:
      return { label: disposition ?? '?', cls: 'tag' };
  }
}

export default function EventFeed() {
  const [items, setItems] = useState([]);
  const [connected, setConnected] = useState(false);
  const wsRef = useRef(null);
  const retryRef = useRef(0);

  useEffect(() => {
    let closed = false;
    let retryTimer = null;

    function connect() {
      const ws = new WebSocket(WS_URL);
      wsRef.current = ws;

      ws.addEventListener('open', () => {
        setConnected(true);
        retryRef.current = 0;
      });

      ws.addEventListener('close', () => {
        setConnected(false);
        if (closed) return;
        const backoff = Math.min(500 * 2 ** retryRef.current, 8000);
        retryRef.current += 1;
        retryTimer = setTimeout(connect, backoff);
      });

      ws.addEventListener('error', () => {
        try { ws.close(); } catch (_) { /* noop */ }
      });

      ws.addEventListener('message', (ev) => {
        let item;
        try {
          item = JSON.parse(ev.data);
        } catch {
          return;
        }
        setItems((prev) => {
          const next = [item, ...prev];
          if (next.length > MAX_ITEMS) next.length = MAX_ITEMS;
          return next;
        });
      });
    }

    connect();

    return () => {
      closed = true;
      if (retryTimer) clearTimeout(retryTimer);
      try { wsRef.current?.close(); } catch (_) { /* noop */ }
    };
  }, []);

  return (
    <section className="panel">
      <header className="panel-header">
        <h2>Live Events</h2>
        <span className={`conn ${connected ? 'conn-up' : 'conn-down'}`}>
          <span className="dot" /> {connected ? 'Connected' : 'Reconnecting…'}
        </span>
      </header>
      <ul className="feed">
        {items.length === 0 && (
          <li className="feed-empty">Waiting for events… send one via <code>POST /events</code>.</li>
        )}
        {items.map((evt) => {
          const tag = dispositionTag(evt.disposition);
          const isAlert = Boolean(evt.alert);
          return (
            <li
              key={evt.event_id + Math.random()}
              className={`feed-item ${isAlert ? 'feed-item-alert' : ''}`}
            >
              <div className="feed-row">
                <span className={`${tag.cls}`}>{tag.label}</span>
                <span className="feed-id">{String(evt.event_id ?? '').slice(-8)}</span>
              </div>
              <div className="feed-row feed-main">
                <strong>{String(evt.source_id ?? '—')}</strong>
                <span className="feed-type">{String(evt.event_type ?? '')}</span>
                <span className="feed-value">{String(evt.value ?? '')}</span>
              </div>
              {isAlert && <div className="feed-alert-reason">⚠ {evt.alert}</div>}
              <div className="feed-ts">{String(evt.timestamp ?? '')}</div>
            </li>
          );
        })}
      </ul>
    </section>
  );
}
