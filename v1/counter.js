/**
 * How many people watching: a drop-in live viewer counter that needs no server.
 *
 * Usage
 *
 *   <span data-viewers></span>
 *   <script src="https://now.rocknroll17.com/v1/counter.js" defer></script>
 *
 *   [data-viewers]        Filled with the current count ("3", "1.2K").
 *   [data-viewers-badge]  Rendered as a ready-made "3 watching now" badge.
 *
 *   Both get data-state="connecting" | "live" | "offline" for styling.
 *   document fires a "viewers:change" event with { count, exact } in detail,
 *   and window.HowManyPeopleWatching exposes the latest values.
 *
 *   Script tag options:
 *     data-key     What to count. Defaults to the page's host and path, so each
 *                  page has its own count. Pages that share a key share a count.
 *     data-broker  MQTT-over-WebSocket broker URL.
 *
 * How it works
 *
 *   If every visitor talked to every other visitor, traffic would grow with the
 *   square of the audience. Instead, only a random sample of "reps" (about
 *   TARGET_REPS of them) exchange heartbeats, and everyone else just listens to
 *   the number the leader announces.
 *
 *   - Each visitor becomes a rep with probability p, so the audience is about
 *     (number of reps) / p.
 *   - The leader is the first eligible rep by ID. It publishes the estimate and
 *     the next p as a retained message, so newcomers receive the current number
 *     the moment they connect.
 *   - While the audience is at most TARGET_REPS, p = 1: everyone is a rep and
 *     the count is exact. Beyond that the error is roughly ±1/√TARGET_REPS.
 *
 *   The broker only relays messages and keeps the last announcement.
 */
(() => {
  'use strict';

  if (window.HowManyPeopleWatching) return; // Included twice.

  const MQTT_LIBRARY = 'https://unpkg.com/mqtt@5.10.1/dist/mqtt.min.js';
  const DEFAULT_BROKER = 'wss://broker.hivemq.com:8884/mqtt';

  const TARGET_REPS = 250;               // Up to this many viewers, the count is exact.
  const BEAT_INTERVAL = 10_000;          // Rep heartbeat period (±20% jitter).
  const BEAT_TTL = BEAT_INTERVAL * 2.5;  // A rep is gone after this long without a heartbeat.
  const HIDDEN_BEAT_TTL = 150_000;       // Browsers throttle timers in hidden tabs to about once a minute.
  const ANNOUNCE_INTERVAL = 5_000;       // Announcements reach everyone, so keep them sparse at scale.
  const ANNOUNCE_INTERVAL_EXACT = 2_000;
  const TAKEOVER_DELAY = ANNOUNCE_INTERVAL + 2_000; // Silence before the next rep in line takes over.
  const LEADER_TIMEOUT = 15_000;         // No announcement for this long means there is no leader at all.
  const ANNOUNCEMENT_TTL = 20_000;       // How long a received announcement is good for display.
  const HELLO_WAIT = 2_500;              // How long a newcomer without an announcement waits for replies.
  const WARMUP = BEAT_INTERVAL * 1.3;    // Long enough to hear every rep's heartbeat at least once.
  const RENDER_INTERVAL = 1_500;         // Minimum time between visible updates.
  const ESTIMATE_DEADBAND = 0.04;        // Estimates that move less than this are not redrawn.
  const ID_PATTERN = /^[a-z0-9]{4,16}$/;

  // ---------------------------------------------------------------------------
  // Options
  // ---------------------------------------------------------------------------

  const scriptTag = document.currentScript;

  function defaultKey() {
    const path = location.pathname.replace(/\/index\.html?$/, '/').replace(/\/+$/, '');
    return location.host + path;
  }

  const key = (scriptTag && scriptTag.dataset.key) || defaultKey();
  const brokerUrl = (scriptTag && scriptTag.dataset.broker) || DEFAULT_BROKER;

  // '+' and '#' are MQTT wildcards and cannot appear in a topic name.
  const namespace = `hmpw/v1/${key.replace(/[+#\u0000]/g, '_')}`;
  const TOPIC = {
    beat: `${namespace}/beat`,
    leave: `${namespace}/leave`,
    count: `${namespace}/count`,
  };

  // ---------------------------------------------------------------------------
  // State
  // ---------------------------------------------------------------------------

  const myId = Math.random().toString(36).slice(2, 9);
  const ticket = Math.random(); // Rep lottery ticket. Fixed for the lifetime of the page.

  // Only populated while we are a rep: id -> { weight, expiresAt, visible, eligible }
  const reps = new Map();
  let isRep = false;
  let repSince = 0;
  let firstBeatAt = 0;
  let askedForHello = false;
  let wasEligible = false;
  let beatTimer = null;

  let announcement = null; // Last leader announcement: { n, p, exact, receivedAt }
  let currentP = 1;
  let lastLiveAnnouncementAt = 0;
  let lastAnnouncer = null;
  let lastAnnouncedAt = 0;
  let connectedAt = 0;

  let client = null;
  let connectionState = 'connecting';

  const isVisible = () => document.visibilityState === 'visible';

  // ---------------------------------------------------------------------------
  // Estimate and sampling probability
  // ---------------------------------------------------------------------------

  function sumWeights() {
    let total = 0;
    for (const { weight } of reps.values()) total += weight;
    return total;
  }

  // Every rep re-checks its ticket against the latest announced p within about
  // a second, so each one was included with probability p and stands for 1/p
  // people. Weights carried in heartbeats can be up to a beat period old and
  // would make the sum swing after p changes, so prefer the head count.
  // Fall back to the weights while p is being adjusted locally (no leader, or
  // backing off from a crowd), when reps are not all using the same p.
  function estimateAudience() {
    const p = samplingProbability();
    return p === currentP ? reps.size / p : sumWeights();
  }

  function recentAnnouncement() {
    if (!announcement) return null;
    return Date.now() - announcement.receivedAt < ANNOUNCEMENT_TTL ? announcement : null;
  }

  function samplingProbability() {
    const now = Date.now();
    let p = currentP;

    // When announcements stop, quadruple p every 5 seconds. More reps means a
    // new leader emerges; even a tiny p recovers to 1 within about 30 seconds.
    const quietFor = now - Math.max(lastLiveAnnouncementAt, connectedAt);
    if (connectedAt && quietFor > LEADER_TIMEOUT) {
      const steps = Math.floor((quietFor - LEADER_TIMEOUT) / 5_000) + 1;
      p = Math.min(1, p * 4 ** steps);
    }

    // If a crowd arrives before any announcement, everyone becomes a rep.
    // Back off on our own when we can already see too many of them.
    if (isRep) {
      const estimate = reps.size / currentP;
      if (estimate > TARGET_REPS * 1.5) p = Math.min(p, TARGET_REPS / estimate);
    }

    return p;
  }

  // Join or leave the reps. Re-evaluated every second and on every announcement.
  function updateRole() {
    if (!client.connected) return;

    const shouldBeRep = ticket < samplingProbability();

    if (shouldBeRep && !isRep) {
      isRep = true;
      repSince = Date.now();
      // Without an announcement we have no idea who is here, so ask the
      // existing reps to speak up instead of waiting a full heartbeat period.
      askedForHello = !recentAnnouncement();
      client.subscribe([TOPIC.beat, TOPIC.leave], () => {
        sendBeat({ hello: askedForHello });
        firstBeatAt = Date.now();
        scheduleBeat();
      });
    } else if (!shouldBeRep && isRep) {
      isRep = false;
      clearTimeout(beatTimer);
      client.publish(TOPIC.leave, myId, { qos: 0 });
      client.unsubscribe([TOPIC.beat, TOPIC.leave]);
      reps.clear();
    }

    render();
  }

  // ---------------------------------------------------------------------------
  // Heartbeats (reps only)
  // ---------------------------------------------------------------------------

  // A fresh rep does not know every other rep yet, so it must not lead.
  function isEligibleForLeader() {
    if (!isRep) return false;
    const waitFor = askedForHello ? HELLO_WAIT : WARMUP;
    return Date.now() - repSince > waitFor;
  }

  function scheduleBeat(delay = BEAT_INTERVAL * (0.8 + Math.random() * 0.4)) {
    clearTimeout(beatTimer);
    beatTimer = setTimeout(() => {
      sendBeat();
      scheduleBeat();
    }, delay);
  }

  function sendBeat({ hello = false } = {}) {
    if (!client.connected || !isRep) return;

    const visible = isVisible();
    const beat = {
      id: myId,
      w: 1 / Math.max(1e-7, samplingProbability()),
      ttl: visible ? BEAT_TTL : HIDDEN_BEAT_TTL,
      vis: visible,
      ok: isEligibleForLeader(),
    };
    if (hello) beat.hi = 1;

    reps.set(myId, { weight: beat.w, expiresAt: Date.now() + beat.ttl, visible, eligible: beat.ok });
    client.publish(TOPIC.beat, JSON.stringify(beat), { qos: 0 });
  }

  // ---------------------------------------------------------------------------
  // Leader
  // ---------------------------------------------------------------------------

  // Our place in line for leadership: eligible reps, visible tabs first, then
  // by ID. Every rep applies the same rule, so no voting is needed.
  function leaderRank() {
    if (!isEligibleForLeader()) return -1;

    const line = [];
    for (const [id, rep] of reps) {
      if (id !== myId && !rep.eligible) continue;
      line.push({ id, visible: id === myId ? isVisible() : rep.visible });
    }
    line.sort((a, b) => (b.visible - a.visible) || (a.id < b.id ? -1 : a.id > b.id ? 1 : 0));
    return line.findIndex(entry => entry.id === myId);
  }

  function isLeader() {
    const rank = leaderRank();
    if (rank < 0) return false;
    if (rank === 0 || lastAnnouncer === myId) return true;

    // A rep ahead of us may have crashed without saying goodbye, and it stays
    // in our list until its heartbeat times out. Step in once the silence has
    // lasted long enough for everyone ahead of us to have had their turn.
    const quietFor = Date.now() - Math.max(lastLiveAnnouncementAt, connectedAt);
    return quietFor > rank * TAKEOVER_DELAY;
  }

  function announce() {
    const estimate = estimateAudience();
    if (estimate < 1) return;

    const message = {
      n: Math.round(estimate),
      p: estimate <= TARGET_REPS ? 1 : TARGET_REPS / estimate,
      exact: [...reps.values()].every(rep => rep.weight === 1),
      ts: Date.now(),
      by: myId,
    };
    client.publish(TOPIC.count, JSON.stringify(message), { qos: 0, retain: true });
    lastAnnouncedAt = Date.now();
  }

  // ---------------------------------------------------------------------------
  // Incoming messages
  // ---------------------------------------------------------------------------

  function parseJson(payload) {
    try {
      return JSON.parse(payload.toString());
    } catch {
      return null;
    }
  }

  function handleAnnouncement(payload, retained) {
    const msg = parseJson(payload);
    if (!msg) return;
    if (!(msg.n >= 1 && msg.n <= 1e9) || !(msg.p > 0 && msg.p <= 1) || typeof msg.ts !== 'number') return;

    const now = Date.now();
    if (retained) {
      // Left over on the broker. Leaders refresh every few seconds, so an old
      // value means the leader is already gone.
      if (now - msg.ts > ANNOUNCEMENT_TTL) return;
    } else {
      lastLiveAnnouncementAt = now;
      lastAnnouncer = ID_PATTERN.test(msg.by) ? msg.by : null;
    }

    announcement = { n: msg.n, p: msg.p, exact: Boolean(msg.exact), receivedAt: now };
    currentP = msg.p;
    updateRole();
  }

  function handleBeat(payload) {
    const beat = parseJson(payload);
    if (!beat || !ID_PATTERN.test(beat.id)) return;
    if (!(beat.w >= 1 && beat.w <= 1e7) || !(beat.ttl >= 5_000 && beat.ttl <= 180_000)) return;

    reps.set(beat.id, {
      weight: beat.w,
      expiresAt: Date.now() + beat.ttl,
      visible: Boolean(beat.vis),
      eligible: Boolean(beat.ok),
    });

    // A newcomer asked for hellos: reply early, with random delay so the
    // replies do not all land at once.
    if (beat.hi && beat.id !== myId) scheduleBeat(Math.random() * 1_500);
    render();
  }

  function handleLeave(payload) {
    const id = payload.toString();
    if (id === myId) return;
    reps.delete(id);
    render();
  }

  // ---------------------------------------------------------------------------
  // Connection and periodic work
  // ---------------------------------------------------------------------------

  function start(mqtt) {
    client = mqtt.connect(brokerUrl, {
      clientId: `hmpw-${myId}`,
      clean: true,
      keepalive: 30,
      reconnectPeriod: 5_000,
    });

    client.on('message', (topic, payload, packet) => {
      if (topic === TOPIC.count) handleAnnouncement(payload, packet && packet.retain);
      else if (topic === TOPIC.beat && isRep) handleBeat(payload);
      else if (topic === TOPIC.leave && isRep) handleLeave(payload);
    });

    client.on('connect', () => {
      setConnectionState('live');
      connectedAt = Date.now();

      // Clean sessions drop all subscriptions on reconnect, so start over.
      isRep = false;
      reps.clear();

      // Look at the retained announcement before deciding whether to be a rep.
      client.subscribe(TOPIC.count, () => setTimeout(updateRole, 300));
    });

    client.on('close', () => setConnectionState('offline'));

    setInterval(tick, 1_000);

    document.addEventListener('visibilitychange', () => {
      if (!isRep) return;
      sendBeat();
      scheduleBeat();
    });

    window.addEventListener('pagehide', () => {
      if (isRep && client.connected) client.publish(TOPIC.leave, myId, { qos: 0 });
    });
  }

  function tick() {
    const now = Date.now();
    for (const [id, rep] of reps) {
      if (id !== myId && now > rep.expiresAt) reps.delete(id);
    }

    updateRole();

    // Tell the other reps the moment we become eligible to lead.
    const eligible = isEligibleForLeader();
    if (eligible && !wasEligible) {
      sendBeat();
      scheduleBeat();
    }
    wasEligible = eligible;

    const interval = estimateAudience() <= TARGET_REPS ? ANNOUNCE_INTERVAL_EXACT : ANNOUNCE_INTERVAL;
    if (client.connected && isLeader() && now - lastAnnouncedAt >= interval) announce();

    // Pick up elements the page added since the last update (SPA navigation).
    updateElements(false);
  }

  function loadMqtt() {
    if (window.mqtt) return Promise.resolve(window.mqtt);
    return new Promise((resolve, reject) => {
      const tag = document.createElement('script');
      tag.src = MQTT_LIBRARY;
      tag.async = true;
      tag.onload = () => (window.mqtt ? resolve(window.mqtt) : reject(new Error('mqtt did not load')));
      tag.onerror = reject;
      document.head.appendChild(tag);
    });
  }

  // ---------------------------------------------------------------------------
  // What to show
  // ---------------------------------------------------------------------------

  // What to show right now, or null if we do not know yet.
  function valueToShow() {
    const now = Date.now();
    const recent = recentAnnouncement();

    if (isRep && samplingProbability() >= 1) {
      // Exact mode: our own list of reps is the freshest source.
      const warmingUp = now - repSince < WARMUP;

      if (warmingUp && recent) {
        // We may not have heard every rep yet, so lean on the announcement.
        // An announcement received well after our first heartbeat already counts us.
        const includesMe = firstBeatAt && recent.receivedAt > firstBeatAt + 1_500;
        return { n: Math.max(reps.size, recent.n + (includesMe ? 0 : 1)), exact: true };
      }
      if (warmingUp && askedForHello && now - repSince < HELLO_WAIT) return null;
      return { n: reps.size, exact: true };
    }

    if (recent) return { n: recent.n, exact: recent.exact };
    if (isRep && now - repSince > HELLO_WAIT) return { n: estimateAudience(), exact: false };
    return null;
  }

  function compactCount(n, exact) {
    if (exact) return n.toLocaleString('en-US');
    if (n < 1_000) return String(Math.round(n / 10) * 10);
    if (n < 1_000_000) return `${(n / 1_000).toFixed(n < 100_000 ? 1 : 0).replace(/\.0$/, '')}K`;
    return `${(n / 1_000_000).toFixed(1).replace(/\.0$/, '')}M`;
  }

  const MESSAGES = {
    ko: {
      alone: '지금은 혼자 보고 있어요',
      watching: count => `현재 <strong class="hmpw-count">${count}</strong>이 함께 보고 있어요`,
      exact: n => `${n.toLocaleString('ko-KR')}명`,
      estimate: n => {
        if (n < 1_000) return `${Math.round(n / 10) * 10}명`;
        if (n < 100_000) return `${(Math.round(n / 100) * 100).toLocaleString('ko-KR')}명`;
        const tenThousands = n / 10_000;
        const text = tenThousands < 1_000
          ? tenThousands.toFixed(1).replace(/\.0$/, '')
          : Math.round(tenThousands).toLocaleString('ko-KR');
        return `${text}만 명`;
      },
    },
    en: {
      alone: 'Just you here right now',
      watching: count => `<strong class="hmpw-count">${count}</strong> watching now`,
      exact: n => (n === 1 ? '1 person' : `${n.toLocaleString('en-US')} people`),
      estimate: n => {
        if (n < 1_000) return `${Math.round(n / 10) * 10} people`;
        if (n < 100_000) return `${(Math.round(n / 100) * 100).toLocaleString('en-US')} people`;
        const [value, unit] = n < 1_000_000 ? [n / 1_000, 'K'] : [n / 1_000_000, 'M'];
        return `${value.toFixed(value < 100 ? 1 : 0).replace(/\.0$/, '')}${unit} people`;
      },
    },
  };
  const text = (navigator.language || '').toLowerCase().startsWith('ko') ? MESSAGES.ko : MESSAGES.en;

  function badgeHtml(n, exact) {
    if (exact && n <= 1) return text.alone;
    return text.watching(exact ? text.exact(n) : text.estimate(n));
  }

  // ---------------------------------------------------------------------------
  // Rendering
  // ---------------------------------------------------------------------------

  // Redrawing on every change makes the number flicker, so changes are
  // batched and applied at most once per RENDER_INTERVAL.
  let shown = null;       // What is on screen: { n, exact, compact, html }
  let pending = null;     // What to draw next
  let renderTimer = null;
  let lastRenderedAt = 0;

  function render() {
    const value = valueToShow();
    if (!value) return; // Keep the placeholder.

    const n = Math.max(1, Math.round(value.n));

    // Estimates wobble with sampling noise; ignore small moves.
    if (!value.exact && shown && !shown.exact && Math.abs(n - shown.n) / shown.n < ESTIMATE_DEADBAND) return;

    pending = { n, exact: value.exact, compact: compactCount(n, value.exact), html: badgeHtml(n, value.exact) };
    if (shown && sameDisplay(pending, shown) && !renderTimer) return;

    if (!shown) {
      paint(); // Show the first number right away.
      return;
    }
    if (!renderTimer) {
      const wait = Math.max(0, RENDER_INTERVAL - (Date.now() - lastRenderedAt));
      renderTimer = setTimeout(paint, wait);
    }
  }

  function sameDisplay(a, b) {
    return a.compact === b.compact && a.html === b.html;
  }

  function paint() {
    renderTimer = null;
    if (shown && sameDisplay(pending, shown)) return;

    shown = pending;
    lastRenderedAt = Date.now();
    updateElements(true);
    document.dispatchEvent(new CustomEvent('viewers:change', { detail: { count: shown.n, exact: shown.exact } }));
  }

  function setConnectionState(state) {
    connectionState = state;
    updateElements(false);
  }

  function updateElements(animate) {
    for (const el of document.querySelectorAll('[data-viewers]')) {
      if (el.dataset.state !== connectionState) el.dataset.state = connectionState;
      if (shown && el.textContent !== shown.compact) el.textContent = shown.compact;
    }

    for (const el of document.querySelectorAll('[data-viewers-badge]')) {
      const label = mountBadge(el);
      if (el.dataset.state !== connectionState) el.dataset.state = connectionState;
      if (!shown || label.hmpwHtml === shown.html) continue;

      const html = shown.html;
      const isFirst = label.hmpwHtml === undefined;
      label.hmpwHtml = html;
      if (isFirst || !animate) {
        label.innerHTML = html;
        continue;
      }
      // A short fade keeps the number from jumping.
      label.classList.add('hmpw-changing');
      setTimeout(() => {
        label.innerHTML = html;
        label.classList.remove('hmpw-changing');
      }, 200);
    }
  }

  function mountBadge(el) {
    let label = el.querySelector('.hmpw-label');
    if (label) return label;

    injectBadgeStyles();
    el.classList.add('hmpw-badge');
    el.innerHTML = '<span class="hmpw-dot" aria-hidden="true"></span>'
      + '<span class="hmpw-label"><span class="hmpw-skeleton" aria-hidden="true"></span></span>';
    return el.querySelector('.hmpw-label');
  }

  function injectBadgeStyles() {
    if (document.getElementById('hmpw-styles')) return;

    const style = document.createElement('style');
    style.id = 'hmpw-styles';
    style.textContent = `
      .hmpw-badge {
        --hmpw-bg: #ffffff;
        --hmpw-text: #334155;
        --hmpw-strong: #0f172a;
        --hmpw-live: #22c55e;
        --hmpw-idle: #94a3b8;
        --hmpw-skeleton: #e2e8f0;
        --hmpw-skeleton-highlight: #f1f5f9;
        display: inline-flex;
        align-items: center;
        gap: 8px;
        padding: 12px 20px;
        border-radius: 9999px;
        background: var(--hmpw-bg);
        box-shadow: 0 4px 6px -1px rgb(0 0 0 / 0.1);
        color: var(--hmpw-text);
        font-size: 15px;
        white-space: nowrap;
      }
      @media (prefers-color-scheme: dark) {
        .hmpw-badge {
          --hmpw-bg: #1e293b;
          --hmpw-text: #cbd5e1;
          --hmpw-strong: #f8fafc;
          --hmpw-idle: #64748b;
          --hmpw-skeleton: #334155;
          --hmpw-skeleton-highlight: #475569;
        }
      }
      .hmpw-dot {
        flex: none;
        width: 10px;
        height: 10px;
        border-radius: 50%;
        background: var(--hmpw-idle);
        transition: background 0.3s;
      }
      .hmpw-badge[data-state="live"] .hmpw-dot {
        background: var(--hmpw-live);
        animation: hmpw-pulse 2s infinite;
      }
      .hmpw-count {
        color: var(--hmpw-strong);
        font-variant-numeric: tabular-nums;
      }
      .hmpw-label { transition: opacity 0.25s; }
      .hmpw-label.hmpw-changing { opacity: 0; }
      .hmpw-skeleton {
        display: inline-block;
        width: 9em;
        height: 1em;
        border-radius: 6px;
        vertical-align: -0.15em;
        background: linear-gradient(90deg, var(--hmpw-skeleton) 0%, var(--hmpw-skeleton-highlight) 50%, var(--hmpw-skeleton) 100%);
        background-size: 200% 100%;
        animation: hmpw-shimmer 1.2s infinite linear;
      }
      @keyframes hmpw-pulse {
        0%, 100% { opacity: 1; transform: scale(1); }
        50% { opacity: 0.4; transform: scale(0.9); }
      }
      @keyframes hmpw-shimmer {
        from { background-position: 200% 0; }
        to { background-position: -200% 0; }
      }
      @media (prefers-reduced-motion: reduce) {
        .hmpw-badge[data-state="live"] .hmpw-dot, .hmpw-skeleton { animation: none; }
        .hmpw-label { transition: none; }
      }
    `;
    document.head.appendChild(style);
  }

  // ---------------------------------------------------------------------------
  // Public API and startup
  // ---------------------------------------------------------------------------

  window.HowManyPeopleWatching = {
    key,
    get count() { return shown ? shown.n : null; },
    get exact() { return shown ? shown.exact : null; },
    get state() { return connectionState; },
  };

  updateElements(false);
  loadMqtt().then(start).catch(() => setConnectionState('offline'));
})();
