# How many people are watching

A drop-in live viewer counter for any web page. There is no backend: browsers
coordinate through a public MQTT broker, so the script is served straight from
GitHub Pages at [now.rocknroll17.com](https://now.rocknroll17.com).

## Usage

```html
<span data-viewers></span>
<script src="https://now.rocknroll17.com/v1/counter.js" defer></script>
```

- `[data-viewers]` is filled with the current count, such as `3` or `1.2K`.
- `[data-viewers-badge]` is rendered as a ready-made "3 watching now" badge.
- Both get `data-state="connecting" | "live" | "offline"` for styling.
- `document` fires a `viewers:change` event with `{ count, exact }` in `detail`, and
  `window.HowManyPeopleWatching` holds the latest values.

Each page is counted separately, keyed by its host and path. To share one count across
pages, or to pick your own key, set it on the script tag:

```html
<script src="https://now.rocknroll17.com/v1/counter.js" data-key="my-site" defer></script>
```

`data-broker` points the counter at another MQTT-over-WebSocket broker.

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

If the leader disappears, the next rep in line takes over after a few seconds of
silence. If announcements stop altogether, visitors raise `p` until a new leader
emerges.

## Files

- `v1/counter.js`: the widget, including counting logic and the badge styles
- `index.html`, `style.css`: the demo page at now.rocknroll17.com, which uses the widget

The `v1` path is part of the contract: incompatible changes go to a new path so pages
already using the widget keep working.

## Limitations

- It depends on a free public broker with no uptime guarantee. If the broker is down,
  the count does not appear.
- Topics are public, so anyone can publish a fake count.
- Viewers whose tab crashes or loses its connection disappear from the count after
  about 25 seconds. Closing the tab normally removes them right away.
