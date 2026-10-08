'use strict';
/* Ifrit online: static file host + WebSocket relay.
   The game is client-authoritative for fights (the victim decides if it was hit),
   so the server only validates, throttles and relays. */
const path = require('path');
const http = require('http');
const fs = require('fs');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const MAX_PLAYERS = 32;
const TICK_MS = 66;               // ~15 snapshots/sec
const MAX_MSG_BYTES = 2048;
const FORMS = ['bedouin', 'keru', 'latin', 'sultan', 'king', 'queen', 'ifrit'];

const PUBLIC = path.join(__dirname, 'public');
const MIME = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript', '.css': 'text/css', '.json': 'application/json',
  '.mp3': 'audio/mpeg', '.ogg': 'audio/ogg', '.wav': 'audio/wav', '.png': 'image/png', '.jpg': 'image/jpeg', '.jpeg': 'image/jpeg',
  '.gif': 'image/gif', '.svg': 'image/svg+xml', '.ico': 'image/x-icon', '.glb': 'model/gltf-binary', '.txt': 'text/plain' };

function serveStatic(req, res) {
  let p;
  try { p = decodeURIComponent(new URL(req.url, 'http://x').pathname); } catch (e) { res.writeHead(400); return res.end(); }
  if (p === '/health') { res.writeHead(200, { 'Content-Type': 'application/json' }); return res.end(JSON.stringify({ ok: true, players: players.size })); }
  if (p.endsWith('/')) p += 'index.html';
  const file = path.normalize(path.join(PUBLIC, p));
  if (!file.startsWith(PUBLIC + path.sep)) { res.writeHead(403); return res.end(); }
  fs.stat(file, (err, st) => {
    if (err || !st.isFile()) { res.writeHead(404, { 'Content-Type': 'text/plain' }); return res.end('Not found'); }
    const ext = path.extname(file).toLowerCase();
    const headers = { 'Content-Type': MIME[ext] || 'application/octet-stream', 'Content-Length': st.size,
      'Cache-Control': ext === '.html' ? 'no-cache' : 'public, max-age=3600' };
    if (req.method === 'HEAD') { res.writeHead(200, headers); return res.end(); }
    res.writeHead(200, headers);
    fs.createReadStream(file).pipe(res);
  });
}

const server = http.createServer(serveStatic);
const wss = new WebSocketServer({ server, maxPayload: MAX_MSG_BYTES });

const players = new Map();        // id -> { ws, state, dirty, lastState, lastSlash, alive }
let nextId = 1;

const num = (v, lim = 1e6) => (typeof v === 'number' && isFinite(v)) ? Math.max(-lim, Math.min(lim, v)) : undefined;
const str = (v, max = 24) => (typeof v === 'string') ? v.slice(0, max) : undefined;

function cleanMount(m) {
  if (!m || typeof m !== 'object') return null;
  const t = str(m.type, 10);
  if (!['carpet', 'horse', 'boat'].includes(t)) return null;
  const o = { type: t };
  for (const k of ['y', 'yaw', 'pitch', 'scale']) { const n = num(m[k]); if (n !== undefined) o[k] = n; }
  if (m.variant !== undefined) o.variant = str(m.variant);
  if (m.gait !== undefined) o.gait = str(m.gait, 10);
  if (m.galleon !== undefined) o.galleon = !!m.galleon;
  if (m.grassy !== undefined) o.grassy = !!m.grassy;
  return o;
}
function cleanState(s) {
  if (!s || typeof s !== 'object') return null;
  const x = num(s.x), z = num(s.z);
  if (x === undefined || z === undefined) return null;
  const o = { x, z, form: FORMS.includes(s.form) ? s.form : 'bedouin' };
  for (const k of ['y', 'yaw', 'pitch']) { const n = num(s[k]); if (n !== undefined) o[k] = n; }
  o.mount = cleanMount(s.mount);
  o.drawn = !!s.drawn;
  o.moving = !!s.moving;
  o.onMindor = !!s.onMindor;
  const isl = num(s.island, 1000); if (isl !== undefined) o.island = isl;
  return o;
}
function cleanSlash(p) {
  if (!p || typeof p !== 'object') return {};
  const o = {};
  for (const k of ['x', 'y', 'z', 'yaw']) { const n = num(p[k]); if (n !== undefined) o[k] = n; }
  o.onMindor = !!p.onMindor;
  o.mount = cleanMount(p.mount);
  return o;
}

function send(ws, obj) { if (ws.readyState === 1) ws.send(JSON.stringify(obj)); }
function broadcast(obj, exceptId) {
  const msg = JSON.stringify(obj);
  for (const [id, p] of players) if (id !== exceptId && p.ws.readyState === 1) p.ws.send(msg);
}

wss.on('connection', (ws) => {
  if (players.size >= MAX_PLAYERS) { send(ws, { type: 'full' }); ws.close(1013, 'server full'); return; }
  const id = 'p' + (nextId++);
  const me = { ws, state: null, dirty: false, lastState: 0, lastSlash: 0, isAlive: true };
  players.set(id, me);
  ws.on('pong', () => { me.isAlive = true; });

  send(ws, {
    type: 'welcome', id,
    players: [...players].filter(([pid, p]) => pid !== id && p.state).map(([pid, p]) => ({ id: pid, s: p.state })),
    count: players.size
  });
  broadcast({ type: 'count', n: players.size });

  ws.on('message', (data) => {
    let m; try { m = JSON.parse(data.toString()); } catch (e) { return; }
    if (!m || typeof m.type !== 'string') return;
    const now = Date.now();
    if (m.type === 'state') {
      if (now - me.lastState < 25) return;           // max ~40/s
      me.lastState = now;
      const s = cleanState(m.s); if (!s) return;
      const first = !me.state;
      me.state = s; me.dirty = true;
      if (first) broadcast({ type: 'join', id, s }, id);
    } else if (m.type === 'slash') {
      if (now - me.lastSlash < 200) return;          // swing cooldown guard
      me.lastSlash = now;
      broadcast({ type: 'slash', id, p: cleanSlash(m.p) }, id);
    } else if (m.type === 'death') {
      broadcast({ type: 'killed', id, by: players.has(m.by) ? m.by : null }, id);
    } else if (m.type === 'ping') {
      send(ws, { type: 'pong', c: num(m.c) });
    }
  });
  const drop = () => {
    if (!players.delete(id)) return;
    broadcast({ type: 'leave', id });
    broadcast({ type: 'count', n: players.size });
  };
  ws.on('close', drop);
  ws.on('error', drop);
});

// Batched snapshots: one message per client containing only players that changed.
setInterval(() => {
  const changed = [];
  for (const [id, p] of players) if (p.dirty && p.state) { changed.push([id, p.state]); p.dirty = false; }
  if (!changed.length) return;
  for (const [id, p] of players) {
    if (p.ws.readyState !== 1) continue;
    const list = changed.filter(([cid]) => cid !== id).map(([cid, s]) => ({ id: cid, s }));
    if (list.length) p.ws.send(JSON.stringify({ type: 'snap', list }));
  }
}, TICK_MS);

// Keepalive (Render closes idle sockets ~55s) + reap dead connections.
setInterval(() => {
  for (const [, p] of players) {
    if (!p.isAlive) { p.ws.terminate(); continue; }
    p.isAlive = false;
    try { p.ws.ping(); } catch (e) {}
  }
}, 25000);

server.listen(PORT, () => console.log('Ifrit online listening on :' + PORT));
