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
    id: p.id, name: p.name, loc: p.loc, level: p.level, score: p.score, catches: p.catches
  }));
}
function broadcast(room, obj, exceptId) {
  for (const p of room.players.values()) if (p.id !== exceptId) wsSend(p.sock, obj);
}
function pushPlayers(room) { broadcast(room, { t: 'players', players: roomList(room) }); }

function leaveRoom(player) {
  const room = player.room && rooms.get(player.room);
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
  switch (m.t) {
    case 'pos': {
      p.bx = int(m.bx, 0, 360, p.bx);
      const loc = int(m.loc, 0, 30, p.loc);
      const level = int(m.level, 1, 9999, p.level);
      const changed = loc !== p.loc || level !== p.level;
      p.loc = loc; p.level = level;
      broadcast(room, { t: 'pos', id: p.id, bx: p.bx, loc: p.loc }, p.id);
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
