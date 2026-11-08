# How many people watching

A tiny static page that shows how many people are viewing it right now. There is no
backend: browsers coordinate through a public MQTT broker, so it can be hosted on
GitHub Pages as-is.

## How it works

Having every visitor talk to every other visitor would make traffic grow with the
square of the audience. Instead, the count is estimated from a random sample.

1. Each visitor draws a random ticket and becomes a **rep** if the ticket is below the
   current sampling probability `p`.
2. Reps send a heartbeat every 10 seconds and track each other. Everyone else only
   listens.
3. The eligible rep with the smallest ID acts as **leader**. It announces
   `reps / p` as the audience size and sets the next `p = 250 / audience`, so the
   number of reps stays around 250 no matter how many people are watching.
4. The announcement is a retained message, so a new visitor sees the current number
   as soon as it connects.

Up to 250 viewers everyone is a rep and the count is exact. Above that it is an
estimate, accurate to roughly ±6%, and shown as an approximate number.

If the leader disappears, the next rep takes over. If announcements stop altogether,
visitors raise `p` until a new leader emerges.

## Files

- `index.html`: markup
- `style.css`: styles, with dark mode and reduced-motion support
- `presence.js`: counting logic and display

## Configuration

The settings are constants at the top of `presence.js`:

- `BROKER_URL`: any MQTT broker that accepts WebSocket connections
- `NAMESPACE`: topic prefix; change it to run a separate counter
- `TARGET_REPS`: sample size; larger is more accurate but sends more messages

## Limitations

- It depends on a free public broker with no uptime guarantee. If the broker is down,
  the count does not appear.
- Topics are public, so anyone can publish a fake count.
- Viewers whose tab crashes or loses its connection disappear from the count after
  about 25 seconds. Closing the tab normally removes them right away.
