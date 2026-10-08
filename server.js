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
  o.place = (typeof s.place === 'string' && s.place.length <= 64) ? s.place : 'out';
  const isl = num(s.island, 1000); if (isl !== undefined) o.island = isl;
  return o;
}
function cleanSlash(p) {
  if (!p || typeof p !== 'object') return {};
  const o = {};
  for (const k of ['x', 'y', 'z', 'yaw']) { const n = num(p[k]); if (n !== undefined) o[k] = n; }
  o.onMindor = !!p.onMindor;
  o.place = (typeof p.place === 'string' && p.place.length <= 64) ? p.place : 'out';
  o.mount = cleanMount(p.mount);
  return o;
}

function send(ws, obj) { if (ws.readyState === 1) ws.send(JSON.stringify(obj)); }
function broadcast(obj, exceptId) {
  const msg = JSON.stringify(obj);
  for (const [id, p] of players) if (id !== exceptId && p.ws.readyState === 1) p.ws.send(msg);
}

// ---------- Adella tables (player vs player) ----------
const tables = new Map();   // id -> { seats:[pid|null,pid|null], purse:[0,0], gameId, live, lastSetter, timer }
function getTable(id) {
  let t = tables.get(id);
  if (!t) { t = { id, seats: [null, null], purse: [0, 0], gameId: 0, live: false, lastSetter: -1, timer: null }; tables.set(id, t); }
  return t;
}
function sendTo(pid, obj) { const p = players.get(pid); if (p) send(p.ws, obj); }
function startGame(t) {
  t.timer = null;
  if (!t.seats[0] || !t.seats[1] || !players.has(t.seats[0]) || !players.has(t.seats[1])) return;
  const m = Math.min(t.purse[0], t.purse[1]);
  const stake = m > 0 ? 1 + Math.floor(Math.random() * m) : 0;
  t.lastSetter = t.lastSetter < 0 ? (Math.random() < 0.5 ? 0 : 1) : 1 - t.lastSetter;
  t.gameId++; t.live = true;
  const msg = { type: 'adl-start', table: t.id, gameId: t.gameId, stake, setter: t.lastSetter };
  sendTo(t.seats[0], msg); sendTo(t.seats[1], msg);
}
function leaveTable(pid) {
  const me = players.get(pid); if (!me || !me.table) return;
  const t = tables.get(me.table); const seat = me.seat;
  me.table = null; me.seat = -1;
  if (!t || t.seats[seat] !== pid) return;
  t.seats[seat] = null;
  const other = t.seats[1 - seat];
  if (t.timer) { clearTimeout(t.timer); t.timer = null; }
  if (other) {
    if (t.live) sendTo(other, { type: 'adl-forfeit', table: t.id, gameId: t.gameId });
    sendTo(other, { type: 'adl-opp', table: t.id, seated: false });
  }
  t.live = false;
  if (!t.seats[0] && !t.seats[1]) tables.delete(t.id);
}
const intIn = (v, lo, hi) => (Number.isFinite(v) ? Math.max(lo, Math.min(hi, Math.trunc(v))) : undefined);
function cleanAct(a) {
  if (!a || typeof a !== 'object' || typeof a.k !== 'string') return null;
  const o = { k: a.k.slice(0, 12) };
  if (typeof a.id === 'string') o.id = a.id.slice(0, 4);
  if (typeof a.point === 'string') o.point = a.point.slice(0, 3);
  for (const k of ['idx', 'col', 'row']) { const n = intIn(a[k], -20, 20); if (n !== undefined) o[k] = n; }
  return o;
}
function handleAdella(id, me, m) {
  if (m.type === 'adl-sit') {
    if (typeof m.table !== 'string' || m.table.length > 64 || (m.seat !== 0 && m.seat !== 1)) return;
    if (me.table) leaveTable(id);
    const t = getTable(m.table);
    const occ = t.seats[m.seat];
    if (occ && occ !== id && players.has(occ)) { send(me.ws, { type: 'adl-denied', table: t.id }); return; }
    t.seats[m.seat] = id; me.table = t.id; me.seat = m.seat;
    t.purse[m.seat] = intIn(m.purse, 0, 1e7) || 0;
    const other = t.seats[1 - m.seat];
    send(me.ws, { type: 'adl-seat', table: t.id, seat: m.seat, opp: !!other });
    if (other) {
      sendTo(other, { type: 'adl-opp', table: t.id, seated: true });
      if (!t.live && !t.timer) t.timer = setTimeout(() => startGame(t), 1200);
    }
  } else if (m.type === 'adl-leave') {
    leaveTable(id);
  } else {
    const t = me.table && tables.get(me.table);
    if (!t || m.table !== t.id) return;
    if (m.type === 'adl-act') {
      if (!t.live || m.gameId !== t.gameId) return;
      const a = cleanAct(m.a); if (!a) return;
      const other = t.seats[1 - me.seat];
      if (other) sendTo(other, { type: 'adl-act', table: t.id, gameId: t.gameId, a });
    } else if (m.type === 'adl-over') {
      if (m.gameId !== t.gameId) return;
      t.purse[me.seat] = intIn(m.purse, 0, 1e7) || 0;
      if (t.live) {
        t.live = false;
        if (t.seats[0] && t.seats[1] && !t.timer) t.timer = setTimeout(() => startGame(t), 3500);
      }
    }
  }
}

// ---------- Remains left where a player is killed (skeleton + loot sack) ----------
const corpses = new Map();      // id -> { id, victim, place, x, y, z, yaw, items, born }
let nextCorpse = 1;
const CORPSE_TTL_MS = 10 * 60 * 1000, MAX_CORPSES = 64;
function corpseView(c) {
  return { id: c.id, victim: c.victim, place: c.place, x: c.x, y: c.y, z: c.z, yaw: c.yaw, items: c.items, age: (Date.now() - c.born) / 1000 };
}
function makeCorpse(victimId, rem) {
  if (!rem || typeof rem !== 'object') return;
  const x = num(rem.x), y = num(rem.y), z = num(rem.z);
  if (x === undefined || y === undefined || z === undefined) return;
  const place = (typeof rem.place === 'string' && rem.place.length <= 64) ? rem.place : 'out';
  const gold = intIn(rem.gold, 0, 100000) || 0;
  const items = [];
  const tre = Math.floor(gold / 10), orn = gold % 10;
  const nT = Math.min(tre, 40);
  for (let i = 0; i < nT; i++) {
    const base = Math.floor(tre / nT), extra = tre % nT;
    items.push({ id: 'g' + i, kind: 'gold', value: (base + (i < extra ? 1 : 0)) * 10 });
  }
  for (let k = 0; k < orn; k++) items.push({ id: 'o' + k, kind: 'gold', value: 1 });
  const hs = (rem.heads && typeof rem.heads === 'object') ? rem.heads : {};
  for (const kind of ['ifrit', 'spriggan', 'bucca']) {
    const n = intIn(hs[kind], 0, 30) || 0;
    for (let k = 0; k < n; k++) items.push({ id: 'h' + kind[0] + k, kind: 'head', head: kind });
  }
  const c = { id: 'c' + (nextCorpse++), victim: victimId, place, x, y, z, yaw: num(rem.yaw) || 0, items, born: Date.now() };
  corpses.set(c.id, c);
  while (corpses.size > MAX_CORPSES) {
    const oldest = corpses.keys().next().value;
    corpses.delete(oldest); broadcast({ type: 'corpse-gone', id: oldest });
  }
  broadcast({ type: 'corpse', c: corpseView(c) });
}
setInterval(() => {
  const now = Date.now();
  for (const [id, c] of corpses) if (now - c.born > CORPSE_TTL_MS) { corpses.delete(id); broadcast({ type: 'corpse-gone', id }); }
}, 15000);

wss.on('connection', (ws) => {
  if (players.size >= MAX_PLAYERS) { send(ws, { type: 'full' }); ws.close(1013, 'server full'); return; }
  const id = 'p' + (nextId++);
  const me = { ws, state: null, dirty: false, lastState: 0, lastSlash: 0, isAlive: true, table: null, seat: -1, lastAdl: 0 };
  players.set(id, me);
  ws.on('pong', () => { me.isAlive = true; });

  send(ws, {
    type: 'welcome', id,
    players: [...players].filter(([pid, p]) => pid !== id && p.state).map(([pid, p]) => ({ id: pid, s: p.state })),
    count: players.size
  });
  broadcast({ type: 'count', n: players.size });
  if (corpses.size) send(ws, { type: 'corpses', list: [...corpses.values()].map(corpseView) });

  ws.on('message', (data) => {
    let m; try { m = JSON.parse(data.toString()); } catch (e) { return; }
    if (!m || typeof m.type !== 'string') return;
    me.isAlive = true;
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
      if (now - (me.lastCorpse || 0) > 5000) { me.lastCorpse = now; makeCorpse(id, m.rem); }
    } else if (m.type === 'loot-take') {
      const c = corpses.get(m.corpse);
      if (!c || !c.items.length) { send(ws, { type: 'loot-gone', corpse: String(m.corpse || '') }); return; }
      let gold = 0; const heads = {};
      for (const it of c.items) { if (it.kind === 'gold') gold += it.value || 0; else if (it.kind === 'head') heads[it.head] = (heads[it.head] || 0) + 1; }
      c.items = [];                                     // first taker gets the whole sack; it is gone for good
      send(ws, { type: 'loot-grant', corpse: c.id, gold, heads });
      broadcast({ type: 'loot-gone', corpse: c.id }, id);
    } else if (m.type.startsWith('adl-')) {
      if (now - me.lastAdl < 10) return;               // rate limit
      me.lastAdl = now;
      handleAdella(id, me, m);
    } else if (m.type === 'ping') {
      send(ws, { type: 'pong', c: num(m.c) });
    }
  });
  const drop = () => {
    leaveTable(id);
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

// Dead-connection reaper: ping every 8s, drop anyone who missed the last ping (~8-16s worst case).
// Browsers answer pings on their own even when the tab is throttled, so live tabs are never dropped.
setInterval(() => {
  for (const [, p] of players) {
    if (!p.isAlive) { p.ws.terminate(); continue; }   // 'close' handler removes + announces leave
    p.isAlive = false;
    try { p.ws.ping(); } catch (e) {}
  }
}, 8000);

// Roster sync: everyone gets the authoritative list of connected ids so ghosts and counts self-heal.
setInterval(() => {
  const ids = [...players.keys()];
  broadcast({ type: 'roster', ids });
}, 4000);

server.listen(PORT, () => console.log('Ifrit online listening on :' + PORT));
