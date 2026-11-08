// BeTa of Fish — ห้องเล่นด้วยกัน (ไม่มี dependency ใช้ Node.js 18+ อย่างเดียว)
// รัน: node server.js   (พอร์ตจาก env PORT ไม่งั้นใช้ 3000)
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const PORT = process.env.PORT || 3000;
const GAME_FILE = path.join(__dirname, 'index.html');
const MAX_ROOMS = 200;
const MAX_PLAYERS = 20;
const MAX_FRAME = 16 * 1024;

const rooms = new Map(); // code -> { players: Map(id -> player), dirty: bool }
let nextId = 1;

// ---------- HTTP: เสิร์ฟตัวเกม ----------
const server = http.createServer((req, res) => {
  const url = (req.url || '/').split('?')[0];
  if (url === '/' || url === '/index.html') {
    fs.readFile(GAME_FILE, (err, data) => {
      if (err) { res.writeHead(500); res.end('game file missing'); return; }
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache' });
      res.end(data);
    });
  } else if (url === '/health') {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('ok');
  } else {
    res.writeHead(404); res.end('not found');
  }
});

// ---------- WebSocket (เขียนเองแบบเล็ก) ----------
function wsSend(sock, obj) {
  if (!sock || sock.destroyed || !sock.writable) return;
  const payload = Buffer.from(JSON.stringify(obj));
  const len = payload.length;
  let header;
  if (len < 126) header = Buffer.from([0x81, len]);
  else if (len < 65536) header = Buffer.from([0x81, 126, len >> 8, len & 255]);
  else return;
  sock.write(Buffer.concat([header, payload]));
}

function parseFrames(player, chunk) {
  player.buf = Buffer.concat([player.buf, chunk]);
  while (player.buf.length >= 2) {
    const b = player.buf;
    const fin = (b[0] & 0x80) !== 0;
    const op = b[0] & 0x0f;
    const masked = (b[1] & 0x80) !== 0;
    let len = b[1] & 0x7f;
    let off = 2;
    if (len === 126) { if (b.length < 4) return; len = b.readUInt16BE(2); off = 4; }
    else if (len === 127) { player.sock.destroy(); return; }
    if (!masked) { player.sock.destroy(); return; }
    if (len > MAX_FRAME) { player.sock.destroy(); return; }
    if (b.length < off + 4 + len) return;
    const mask = b.slice(off, off + 4);
    const data = Buffer.from(b.slice(off + 4, off + 4 + len));
    for (let i = 0; i < data.length; i++) data[i] ^= mask[i & 3];
    player.buf = b.slice(off + 4 + len);

    if (op === 0x8) { player.sock.end(); return; }          // close
    if (op === 0x9) { // ping -> pong
      const pong = Buffer.concat([Buffer.from([0x8a, data.length]), data]);
      if (data.length < 126) player.sock.write(pong);
      continue;
    }
    if (op === 0xa) { player.alive = true; continue; }       // pong
    if (op === 0x1 || op === 0x0) {
      player.frag = Buffer.concat([player.frag || Buffer.alloc(0), data]);
      if (player.frag.length > MAX_FRAME) { player.sock.destroy(); return; }
      if (fin) {
        const text = player.frag.toString('utf8');
        player.frag = null;
        let msg;
        try { msg = JSON.parse(text); } catch (e) { continue; }
        if (msg && typeof msg === 'object') handleMessage(player, msg);
      }
    }
  }
}

server.on('upgrade', (req, sock) => {
  const key = req.headers['sec-websocket-key'];
  if (!key || (req.headers.upgrade || '').toLowerCase() !== 'websocket') { sock.destroy(); return; }
  const accept = crypto.createHash('sha1')
    .update(key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64');
  sock.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + accept + '\r\n\r\n');
  sock.setNoDelay(true);
  const player = {
    id: nextId++, sock, buf: Buffer.alloc(0), frag: null, alive: true,
    room: null, name: '', loc: 0, level: 1, bx: 180, score: 0, catches: 0,
    lastChat: 0, lastCatch: 0
  };
  sock.on('data', c => parseFrames(player, c));
  sock.on('close', () => leaveRoom(player));
  sock.on('error', () => { try { sock.destroy(); } catch (e) {} });
});

// ---------- ห้อง ----------
const cleanName = s => String(s || '').replace(/[<>&"'\u0000-\u001f]/g, '').trim().slice(0, 20) || 'ผู้เล่น';
const cleanCode = s => String(s || '').toUpperCase().replace(/[^A-Z0-9]/g, '').slice(0, 12);
const int = (v, lo, hi, d) => { v = Math.floor(Number(v)); return Number.isFinite(v) ? Math.min(hi, Math.max(lo, v)) : d; };

function roomList(room) {
  return [...room.players.values()].map(p => ({
    id: p.id, name: p.name, loc: p.loc, level: p.level, score: p.score, catches: p.catches, st: p.st|0, hy: p.hy||55, rod: p.rod|0
  }));
}
function broadcast(room, obj, exceptId) {
  for (const p of room.players.values()) if (p.id !== exceptId) wsSend(p.sock, obj);
}
function pushPlayers(room) { broadcast(room, { t: 'players', players: roomList(room) }); }

// ---------- ระบบแลกของ (เซิร์ฟเวอร์เป็นตัวกลางนับเวลา + ยืนยันสองฝั่งก่อนสลับ) ----------
const trades = new Map();      // tradeId -> trade
const tradeOf = new Map();     // playerId -> trade
const invites = new Map();     // toId -> { from, exp }
let tradeSeq = 1;
const TRADE_COUNTDOWN_MS = 5000;
const TRADE_READY_TIMEOUT_MS = 6000;

function cleanKey(k) { return String(k).replace(/[\u0000-\u001f<>]/g, '').slice(0, 60); }
function cleanCounts(o, maxKeys) {
  const out = {};
  if (!o || typeof o !== 'object') return out;
  let n = 0;
  for (const k of Object.keys(o)) {
    if (n >= maxKeys) break;
    const v = Math.floor(Number(o[k]));
    if (!Number.isFinite(v) || v < 1) continue;
    out[cleanKey(k)] = Math.min(v, 1e9); n++;
  }
  return out;
}
function sanitizeOffer(o) {
  o = o && typeof o === 'object' ? o : {};
  const offer = { money: 0, rods: [], baits: {}, mats: {}, bag: {}, fish: {}, fishInfo: {}, enchants: {} };
  const money = Math.floor(Number(o.money));
  if (Number.isFinite(money) && money > 0) offer.money = Math.min(money, 1e13);
  if (Array.isArray(o.rods)) {
    const seen = new Set();
    for (const r of o.rods.slice(0, 60)) { const id = int(r, 0, 999, -1); if (id >= 0 && !seen.has(id)) { seen.add(id); offer.rods.push(id); } }
  }
  offer.baits = cleanCounts(o.baits, 20);
  offer.mats = cleanCounts(o.mats, 30);
  offer.bag = cleanCounts(o.bag, 30);
  offer.fish = cleanCounts(o.fish, 60);
  for (const k of Object.keys(offer.fish)) {
    const fi = o.fishInfo && o.fishInfo[k];
    if (fi && typeof fi === 'object') {
      offer.fishInfo[k] = {
        name: cleanKey(fi.name || ''), weight: (typeof fi.weight === 'number') ? fi.weight : cleanKey(fi.weight || '?'),
        price: int(fi.price, 0, 1e9, 0), rarity: cleanKey(fi.rarity || '')
      };
    }
  }
  if (o.enchants && typeof o.enchants === 'object') {
    for (const id of offer.rods) {
      const e = o.enchants[id];
      if (e && typeof e === 'object') { try { const j = JSON.stringify(e); if (j.length < 400) offer.enchants[id] = JSON.parse(j); } catch (e2) {} }
    }
  }
  return offer;
}
function findPlayer(room, id) {
  for (const pl of room.players.values()) if (String(pl.id) === String(id)) return pl;
  return null;
}
function tradeState(t, extra) {
  return Object.assign({ t: 'trade_state', id: t.id, offers: t.offers, acc: t.acc, state: t.state, cdEnd: t.cdEnd || 0 }, extra || {});
}
function tradeSend(t, room, obj) {
  for (const id of [t.a, t.b]) { const pl = findPlayer(room, id); if (pl) wsSend(pl.sock, obj); }
}
function endTrade(t, room, reason, byName) {
  if (!trades.has(t.id)) return;
  clearTimeout(t.timer); clearTimeout(t.readyTimer);
  trades.delete(t.id); tradeOf.delete(t.a); tradeOf.delete(t.b);
  if (room) tradeSend(t, room, { t: 'trade_end', id: t.id, reason, by: byName || '' });
}
function cancelTradesOf(player, reason) {
  const room = player.room && rooms.get(player.room);
  invites.delete(player.id);
  for (const [k, v] of invites) if (v.from === player.id) invites.delete(k);
  const t = tradeOf.get(player.id);
  if (t) endTrade(t, room, reason || 'leave', player.name);
}
function tradeMsg(p, room, m) {
  const now = Date.now();
  switch (m.t) {
    case 'trade_req': {
      const to = findPlayer(room, String(m.to || ''));
      if (!to || to.id === p.id) return;
      if (tradeOf.has(p.id)) return wsSend(p.sock, { t: 'trade_end', reason: 'busy_me' });
      if (tradeOf.has(to.id)) return wsSend(p.sock, { t: 'trade_end', reason: 'busy_other', by: to.name });
      invites.set(to.id, { from: p.id, exp: now + 30000 });
      wsSend(to.sock, { t: 'trade_inv', from: p.id, name: p.name });
      wsSend(p.sock, { t: 'trade_sent', to: to.id, name: to.name });
      break;
    }
    case 'trade_res': {
      const inv = invites.get(p.id);
      const from = findPlayer(room, String(m.from || ''));
      if (!inv || !from || inv.from !== from.id || inv.exp < now) return;
      invites.delete(p.id);
      if (!m.ok) return wsSend(from.sock, { t: 'trade_end', reason: 'declined', by: p.name });
      if (tradeOf.has(p.id) || tradeOf.has(from.id)) return wsSend(from.sock, { t: 'trade_end', reason: 'busy_other', by: p.name });
      const t = { id: 'T' + (tradeSeq++), a: from.id, b: p.id, offers: {}, acc: {}, state: 'open', timer: null, readyTimer: null, ready: {}, cdEnd: 0 };
      t.offers[t.a] = sanitizeOffer({}); t.offers[t.b] = sanitizeOffer({});
      t.acc[t.a] = false; t.acc[t.b] = false;
      trades.set(t.id, t); tradeOf.set(t.a, t); tradeOf.set(t.b, t);
      wsSend(from.sock, { t: 'trade_open', id: t.id, with: p.id, name: p.name });
      wsSend(p.sock, { t: 'trade_open', id: t.id, with: from.id, name: from.name });
      tradeSend(t, room, tradeState(t));
      break;
    }
    case 'trade_offer': {
      const t = tradeOf.get(p.id);
      if (!t || t.state !== 'open') return;
      t.offers[p.id] = sanitizeOffer(m.offer);
      t.acc[t.a] = false; t.acc[t.b] = false;
      tradeSend(t, room, tradeState(t));
      break;
    }
    case 'trade_accept': {
      const t = tradeOf.get(p.id);
      if (!t || t.state !== 'open') return;
      t.acc[p.id] = !!m.on;
      if (t.acc[t.a] && t.acc[t.b]) {
        t.state = 'countdown'; t.cdEnd = now + TRADE_COUNTDOWN_MS;
        t.timer = setTimeout(() => {
          if (!trades.has(t.id)) return;
          t.state = 'verify'; t.ready = {};
          tradeSend(t, room, { t: 'trade_go', id: t.id });
          t.readyTimer = setTimeout(() => endTrade(t, room, 'timeout'), TRADE_READY_TIMEOUT_MS);
        }, TRADE_COUNTDOWN_MS);
      }
      tradeSend(t, room, tradeState(t));
      break;
    }
    case 'trade_ready': {
      const t = tradeOf.get(p.id);
      if (!t || t.state !== 'verify') return;
      if (!m.ok) return endTrade(t, room, 'verify_fail', p.name + (m.why ? ': ' + cleanKey(m.why) : ''));
      t.ready[p.id] = true;
      if (t.ready[t.a] && t.ready[t.b]) {
        const done = { t: 'trade_done', id: t.id, offers: t.offers, a: t.a, b: t.b };
        clearTimeout(t.readyTimer);
        trades.delete(t.id); tradeOf.delete(t.a); tradeOf.delete(t.b);
        tradeSend(t, room, done);
      }
      break;
    }
    case 'trade_cancel': {
      const t = tradeOf.get(p.id);
      if (t) return endTrade(t, room, 'cancel', p.name);
      for (const [k, v] of invites) if (v.from === p.id || k === p.id) {
        invites.delete(k);
        const other = findPlayer(room, v.from === p.id ? k : v.from);
        if (other) wsSend(other.sock, { t: 'trade_end', reason: 'cancel', by: p.name });
      }
      break;
    }
  }
}

function leaveRoom(player) {
  const room = player.room && rooms.get(player.room);
  try { cancelTradesOf(player, 'leave'); } catch (e) {}
  player.room = null;
  if (!room) return;
  room.players.delete(player.id);
  if (room.players.size === 0) rooms.delete(room.code);
  else { pushPlayers(room); broadcast(room, { t: 'sys', text: player.name + ' ออกจากห้อง' }); }
}

function handleMessage(p, m) {
  if (m.t === 'join') {
    const code = cleanCode(m.room);
    if (!code) { wsSend(p.sock, { t: 'error', text: 'รหัสห้องไม่ถูกต้อง' }); return; }
    if (p.room) leaveRoom(p);
    let room = rooms.get(code);
    if (!room) {
      if (rooms.size >= MAX_ROOMS) { wsSend(p.sock, { t: 'error', text: 'เซิร์ฟเวอร์เต็ม ลองใหม่ภายหลัง' }); return; }
      room = { code, players: new Map() };
      rooms.set(code, room);
    }
    if (room.players.size >= MAX_PLAYERS) { wsSend(p.sock, { t: 'error', text: 'ห้องเต็ม' }); return; }
    p.name = cleanName(m.name);
    // ชื่อซ้ำในห้อง -> ต่อท้ายเลข
    const used = new Set([...room.players.values()].map(x => x.name));
    if (used.has(p.name)) { let n = 2; while (used.has(p.name + n)) n++; p.name += n; }
    p.loc = int(m.loc, 0, 30, 0); p.level = int(m.level, 1, 9999, 1);
    p.score = 0; p.catches = 0; p.room = code;
    room.players.set(p.id, p);
    wsSend(p.sock, { t: 'joined', you: p.id, room: code, name: p.name, players: roomList(room) });
    broadcast(room, { t: 'players', players: roomList(room) }, p.id);
    broadcast(room, { t: 'sys', text: p.name + ' เข้าห้องแล้ว' }, p.id);
    return;
  }
  const room = p.room && rooms.get(p.room);
  if (!room) return;
  const now = Date.now();
  if (typeof m.t === 'string' && m.t.startsWith('trade_')) { tradeMsg(p, room, m); return; }
  switch (m.t) {
    case 'pos': {
      p.bx = int(m.bx, 0, 360, p.bx);
      const loc = int(m.loc, 0, 30, p.loc);
      const level = int(m.level, 1, 9999, p.level);
      const changed = loc !== p.loc || level !== p.level;
      p.loc = loc; p.level = level;
      p.st = int(m.st, 0, 6, 0); p.hy = int(m.hy, 0, 400, 55); p.rod = int(m.rod, 0, 99, 0);
      broadcast(room, { t: 'pos', id: p.id, bx: p.bx, loc: p.loc, st: p.st, hy: p.hy, rod: p.rod }, p.id);
      if (changed) pushPlayers(room);
      break;
    }
    case 'chat': {
      if (now - p.lastChat < 600) return;
      p.lastChat = now;
      const text = String(m.text || '').replace(/[\u0000-\u001f]/g, ' ').trim().slice(0, 120);
      if (!text) return;
      broadcast(room, { t: 'chat', id: p.id, from: p.name, text });
      break;
    }
    case 'catch': {
      if (now - p.lastCatch < 1000) return;
      p.lastCatch = now;
      const fish = String(m.fish || '').replace(/[\u0000-\u001f<>]/g, '').slice(0, 40);
      const rarity = String(m.rarity || '').slice(0, 12);
      const weight = Math.round(Number(m.weight) * 100) / 100 || 0;
      const price = int(m.price, 0, 5000000, 0);
      if (!fish) return;
      p.catches++; p.score += price;
      broadcast(room, { t: 'catch', id: p.id, from: p.name, fish, rarity, weight, price });
      pushPlayers(room);
      break;
    }
  }
}

// ส่ง ping ทุก 25 วิ กันเซิร์ฟเวอร์/พร็อกซีตัดการเชื่อมต่อ และเก็บคนหลุด
setInterval(() => {
  for (const room of rooms.values()) {
    for (const p of room.players.values()) {
      if (!p.alive) { p.sock.destroy(); continue; }
      p.alive = false;
      if (p.sock.writable) p.sock.write(Buffer.from([0x89, 0x00]));
    }
  }
}, 25000);

server.listen(PORT, () => console.log('BeTa of Fish server on port ' + PORT));
