# Ifrit Online

`public/index.html` is the game (with the online layer built in). `server.js` hosts it and relays players over WebSockets.

## Folder layout
```
ifrit-online/
  server.js
  package.json
  render.yaml
  .gitignore
  public/
    index.html        <- the game
    *.mp3             <- ALL your sound files (dunes.mp3, ting.mp3, swipe.mp3 ...) go here, next to index.html
```

## Run locally
```
npm install
npm start          # http://localhost:3000  (open it in two tabs to test)
```

## Deploy: GitHub + Render
1. Put this folder in a GitHub repo (`git init`, `git add .`, `git commit`, push). Include your mp3s in `public/`.
2. On render.com: **New > Web Service**, connect the repo.
   - Runtime: Node, Build: `npm install`, Start: `npm start` (render.yaml sets these automatically if you use **Blueprint**).
   - Health check path: `/health`
3. Open your `https://<name>.onrender.com` URL. Everyone who opens it joins the same world.
   (Render's free plan sleeps after idle; the first load may take ~30s to wake.)

Hosting the page elsewhere (e.g. GitHub Pages)? Point it at the server: `index.html?server=wss://<name>.onrender.com`
or set `window.MP_SERVER_URL` before the game script.

## How it works
- Client sends `PlayerForms.localState()` ~15x/sec; server validates, throttles, and sends batched snapshots to the others.
- Remote players are smoothed and drawn through `PlayerForms.updateRemote`; removed with `removeRemote` on leave/disconnect.
- Slashes: `PlayerNet.onLocalSlash` -> relayed -> `PlayerForms.slashRemote`. Slash time is converted to the receiver's `performance.now()/1000` clock (minus half the measured ping), so the 0.35s deflect window works across machines.
- Hits are decided on the victim's client (both on Mindor, within 2.4 m, facing). Death calls `PlayerNet.onLocalDeath`, which the server announces to others.
- Server limits: 32 players, 2KB messages, state/slash rate limits, keepalive pings for Render's idle timeout.
