/**
 * Counts how many people are viewing this page, without a server.
 *
 * If every visitor talked to every other visitor, traffic would grow with the
 * square of the audience. Instead, only a random sample of "reps" (about
 * TARGET_REPS of them) exchange heartbeats, and everyone else just listens to
 * the number the leader announces.
 *
 *  - Each visitor becomes a rep with probability p, and each rep stands in for
 *    1/p people. The sum of the reps' weights, Σ(1/p), estimates the audience.
 *  - The leader is the eligible rep with the smallest ID. It publishes the
 *    estimate and the next p as a retained message, so newcomers receive the
 *    current number the moment they connect.
 *  - While the audience is at most TARGET_REPS, p = 1: everyone is a rep and the
 *    count is exact. Beyond that the error is roughly ±1/√TARGET_REPS.
 *
 * The broker only relays messages and keeps the last announcement.
 */
(() => {
  'use strict';

  const BROKER_URL = 'wss://broker.hivemq.com:8884/mqtt';
  const NAMESPACE = 'rocknroll17/how-many-people-watching/v1';
  const TOPIC = {
    beat: `${NAMESPACE}/beat`,
    leave: `${NAMESPACE}/leave`,
    count: `${NAMESPACE}/count`,
  };

  const TARGET_REPS = 250;               // Up to this many viewers, the count is exact.
  const BEAT_INTERVAL = 10_000;          // Rep heartbeat period (±20% jitter).
  const BEAT_TTL = BEAT_INTERVAL * 2.5;  // A rep is gone after this long without a heartbeat.
  const HIDDEN_BEAT_TTL = 150_000;       // Browsers throttle timers in hidden tabs to about once a minute.
  const ANNOUNCE_INTERVAL = 5_000;       // Announcements reach everyone, so keep them sparse at scale.
  const ANNOUNCE_INTERVAL_EXACT = 2_000;
  const LEADER_TIMEOUT = 15_000;         // No announcement for this long means there is no leader.
  const ANNOUNCEMENT_TTL = 20_000;       // How long a received announcement is good for display.
  const HELLO_WAIT = 2_500;              // How long a newcomer without an announcement waits for replies.
  const WARMUP = BEAT_INTERVAL * 1.3;    // Long enough to hear every rep's heartbeat at least once.
  const RENDER_INTERVAL = 1_500;         // Minimum time between visible updates.
  const ESTIMATE_DEADBAND = 0.04;        // Estimates that move less than this are not redrawn.
  const ID_PATTERN = /^[a-z0-9]{4,16}$/;

  const MESSAGES = {
    ko: {
      locale: 'ko-KR',
      loading: '현재 <span class="skeleton" aria-hidden="true"></span> 함께 보고 있어요',
      alone: '지금은 혼자 보고 있어요',
      watching: count => `현재 <strong class="count">${count}</strong>이 함께 보고 있어요`,
      exact: (n, fmt) => `${fmt(n)}명`,
      about: text => `약 ${text}명`,
      aboutLarge: n => {
        const tenThousands = n / 10_000;
        const text = tenThousands < 1_000
          ? tenThousands.toFixed(1).replace(/\.0$/, '')
          : Math.round(tenThousands).toLocaleString('ko-KR');
        return `약 ${text}만 명`;
      },
    },
    en: {
      locale: 'en-US',
      loading: '<span class="skeleton" aria-hidden="true"></span> watching now',
      alone: 'Just you here right now',
      watching: count => `<strong class="count">${count}</strong> watching now`,
      exact: (n, fmt) => (n === 1 ? '1 person' : `${fmt(n)} people`),
      about: text => `about ${text} people`,
      aboutLarge: n => {
        const [value, unit] = n < 1_000_000 ? [n / 1_000, 'K'] : [n / 1_000_000, 'M'];
        return `about ${value.toFixed(value < 100 ? 1 : 0).replace(/\.0$/, '')}${unit} people`;
      },
    },
  };
  const text = navigator.language.toLowerCase().startsWith('ko') ? MESSAGES.ko : MESSAGES.en;

  const badgeEl = document.getElementById('badge');
  const labelEl = document.getElementById('label');
  document.documentElement.lang = text === MESSAGES.ko ? 'ko' : 'en';
  labelEl.innerHTML = text.loading;

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
  let lastAnnouncedAt = 0;
  let connectedAt = 0;

  const client = mqtt.connect(BROKER_URL, {
    clientId: `hmpw-${myId}`,
    clean: true,
    keepalive: 30,
    reconnectPeriod: 5_000,
  });

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

  // The leader is the eligible rep with the smallest ID, preferring visible
  // tabs. Every rep applies the same rule, so no voting is needed.
  function isLeader() {
    if (!isEligibleForLeader()) return false;

    let firstVisible = null;
    let firstAny = null;
    for (const [id, rep] of reps) {
      const eligible = id === myId || rep.eligible;
      const visible = id === myId ? isVisible() : rep.visible;
      if (!eligible) continue;
      if (firstAny === null || id < firstAny) firstAny = id;
      if (visible && (firstVisible === null || id < firstVisible)) firstVisible = id;
    }
    return (firstVisible ?? firstAny) === myId;
  }

  function announce() {
    const estimate = estimateAudience();
    if (estimate < 1) return;

    const message = {
      n: Math.round(estimate),
      p: estimate <= TARGET_REPS ? 1 : TARGET_REPS / estimate,
      exact: [...reps.values()].every(rep => rep.weight === 1),
      ts: Date.now(),
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

  client.on('message', (topic, payload, packet) => {
    if (topic === TOPIC.count) handleAnnouncement(payload, packet && packet.retain);
    else if (topic === TOPIC.beat && isRep) handleBeat(payload);
    else if (topic === TOPIC.leave && isRep) handleLeave(payload);
  });

  client.on('connect', () => {
    badgeEl.classList.add('live');
    connectedAt = Date.now();

    // Clean sessions drop all subscriptions on reconnect, so start over.
    isRep = false;
    reps.clear();

    // Look at the retained announcement before deciding whether to be a rep.
    client.subscribe(TOPIC.count, () => setTimeout(updateRole, 300));
  });

  client.on('close', () => badgeEl.classList.remove('live'));

  // ---------------------------------------------------------------------------
  // Periodic work
  // ---------------------------------------------------------------------------

  setInterval(() => {
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
  }, 1_000);

  document.addEventListener('visibilitychange', () => {
    if (!isRep) return;
    sendBeat();
    scheduleBeat();
  });

  window.addEventListener('pagehide', () => {
    if (isRep && client.connected) client.publish(TOPIC.leave, myId, { qos: 0 });
  });

  // ---------------------------------------------------------------------------
  // Display
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

  function formatCount(n, exact) {
    const fmt = value => value.toLocaleString(text.locale);
    if (exact) return text.exact(n, fmt);
    if (n < 1_000) return text.about(fmt(Math.round(n / 10) * 10));
    if (n < 100_000) return text.about(fmt(Math.round(n / 100) * 100));
    return text.aboutLarge(n);
  }

  function labelHtml(n, exact) {
    if (exact && n <= 1) return text.alone;
    return text.watching(formatCount(n, exact));
  }

  // Redrawing on every change makes the badge flicker, so changes are batched
  // and applied at most once per RENDER_INTERVAL.
  let shown = null;       // What is on screen: { n, exact, html }
  let pending = null;     // What to draw next
  let renderTimer = null;
  let lastRenderedAt = 0;

  function render() {
    const value = valueToShow();
    if (!value) return; // Keep the skeleton.

    const n = Math.max(1, Math.round(value.n));

    // Estimates wobble with sampling noise; ignore small moves.
    if (!value.exact && shown && !shown.exact && Math.abs(n - shown.n) / shown.n < ESTIMATE_DEADBAND) return;

    pending = { n, exact: value.exact, html: labelHtml(n, value.exact) };
    if (shown && pending.html === shown.html && !renderTimer) return;

    if (!shown) {
      paint(); // Show the first number right away.
      return;
    }
    if (!renderTimer) {
      const wait = Math.max(0, RENDER_INTERVAL - (Date.now() - lastRenderedAt));
      renderTimer = setTimeout(paint, wait);
    }
  }

  function paint() {
    renderTimer = null;
    if (shown && pending.html === shown.html) return;

    const isFirst = !shown;
    shown = pending;
    lastRenderedAt = Date.now();

    if (isFirst) {
      labelEl.innerHTML = shown.html;
      return;
    }

    // A short fade keeps the number from jumping.
    labelEl.classList.add('is-changing');
    setTimeout(() => {
      labelEl.innerHTML = shown.html;
      labelEl.classList.remove('is-changing');
    }, 200);
  }
})();
