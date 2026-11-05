#!/usr/bin/env node
'use strict';
/* =====================================================================
   LÊN CHÙA CẦU AN — máy chủ trung chuyển (relay) · mod by mrwick68 · phiên bản 4
   - Không cần cài thư viện: chỉ cần Node.js >= 18.  Chạy:  node server.js
   - Một cổng duy nhất:
       GET  /              -> index.html (nén gzip, có ETag)
       GET  /three.module.min.js -> bản Three.js đặt cạnh server (tuỳ chọn)
       GET  /health        -> trạng thái JSON (số kết nối, số phòng)
       WS   /ws            -> kênh chơi online
   - Giao thức WebSocket (JSON):
       client -> {t:'hello',v:2,id,room,n,r,h,x,z,y,a}   (bắt buộc đầu tiên, trong 5 giây)   h = kiểu tóc 0..4, a = dáng 0..9 (9 = ngồi thiền)
       client -> {t:'s',x,z,y,a} | {t:'ping',ts} | {t:'e',k:'bell'} | {t:'bye'}
       server -> {t:'welcome',room,peers:[...]} | {t:'join',p} | {t:'leave',id}
                 {t:'snap',d:[[id,x,z,y,a],...]}  (gộp, chỉ gửi phần thay đổi, mặc định 5 lần/giây)
                 {t:'pong',ts} | {t:'ev',id,k} | {t:'full'}
   - Không ghi tên/vị trí ra đĩa, không có cơ sở dữ liệu; mọi thứ chỉ nằm trong RAM.
   ===================================================================== */
const http = require('http'), fs = require('fs'), path = require('path'), crypto = require('crypto'), zlib = require('zlib');

/* ---------- 1. Cấu hình (đặt bằng biến môi trường) ---------- */
const E = process.env;
const num = (v, d, lo, hi) => { const n = Number(v); return (v !== undefined && v !== '' && Number.isFinite(n)) ? Math.min(hi, Math.max(lo, n)) : d; };
const PORT = num(E.PORT, 8080, 0, 65535), HOST = E.HOST || '0.0.0.0';
const MAX_ROOM = num(E.MAX_ROOM, 40, 2, 200);          // số người tối đa mỗi phòng (phòng chung tự tách "chung-2", "chung-3"...)
const MAX_CLIENTS = num(E.MAX_CLIENTS, 500, 1, 5000);  // tổng kết nối tối đa
const MAX_PER_IP = num(E.MAX_PER_IP, 30, 1, 1000);     // kết nối tối đa mỗi IP (mạng di động dùng chung IP nên để rộng)
const TICK_MS = num(E.TICK_MS, 200, 50, 1000);         // chu kỳ gộp trạng thái gửi cho người chơi
const IDLE_MS = num(E.IDLE_MS, 25000, 3000, 300000);   // ngắt kết nối nếu quá thời gian này không nhận được gì (client ping mỗi 5 giây)
const TRUST_PROXY = E.TRUST_PROXY === '1';             // đặt 1 khi chạy sau Caddy/Nginx/Cloudflare/Render để lấy IP thật từ X-Forwarded-For
const ORIGINS = (E.ALLOWED_ORIGINS || '').split(',').map(s => s.trim()).filter(Boolean); // để trống = cho mọi nguồn
const ROOT = path.resolve(E.STATIC_DIR || __dirname);
const MAX_MSG = 1024;                                  // byte tối đa mỗi tin nhắn từ client
const GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
const ID_RE = /^[a-z0-9]{4,16}$/, ROOM_RE = /^[a-z0-9-]{1,16}$/;
const BAD = new Set(['địt', 'đụ', 'đù', 'lồn', 'cặc', 'buồi', 'đéo', 'cứt', 'đĩ', 'dit', 'dm', 'đm', 'dmm', 'đmm', 'dcm', 'đcm', 'dkm', 'đkm', 'vcl', 'vkl', 'vl', 'clgt', 'cmm', 'fuck', 'fucking', 'shit', 'bitch', 'asshole', 'cunt', 'dick']);

/* ---------- 2. Tiện ích ---------- */
const clampN = (v, a, b) => { const n = Number(v); return Number.isFinite(n) ? Math.round(Math.min(b, Math.max(a, n)) * 100) / 100 : 0; };
const poseOf = a => { const n = a | 0; return n >= 0 && n <= 9 ? n : 0; };
const hairOf = h => { const n = Math.round(Number(h)); return Number.isFinite(n) && n >= 0 && n <= 4 ? n : 0; };
function cleanName(n) {
  let s = Array.from(String(n == null ? '' : n).replace(/[\u0000-\u001f\u007f<>]/g, '').replace(/\s+/g, ' ').trim()).slice(0, 16).join('');
  if (!s) return 'Khách';
  const toks = s.toLowerCase().split(/[^\p{L}\p{N}]+/u).filter(Boolean);
  return toks.some(t => BAD.has(t)) ? 'Khách' : s;
}
const log = (...a) => console.log(new Date().toISOString(), ...a);

/* ---------- 3. Phục vụ file tĩnh (chỉ whitelist) ---------- */
const FILES = { '/': 'index.html', '/index.html': 'index.html', '/three.module.min.js': 'three.module.min.js' };
const MIME = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript; charset=utf-8' };
const cache = new Map();
function getFile(name) {
  const fp = path.join(ROOT, name); let st;
  try { st = fs.statSync(fp); } catch (e) { return null; }
  const c = cache.get(name); if (c && c.m === st.mtimeMs) return c;
  const raw = fs.readFileSync(fp);
  const tag = crypto.createHash('sha1').update(raw).digest('base64url').slice(0, 20);
  const o = { m: st.mtimeMs, raw, gz: zlib.gzipSync(raw, { level: 9 }), etag: '"' + tag + '"', etagGz: '"' + tag + '-gz"', type: MIME[path.extname(name)] || 'application/octet-stream' };
  cache.set(name, o); return o;
}

/* ---------- 4. Phòng & kết nối ---------- */
const clients = new Set(), rooms = new Map(), perIp = new Map();
class Room {
  constructor(name) { this.name = name; this.byId = new Map(); this.dirty = new Set(); }
  get size() { return this.byId.size; }
  add(c) {
    const old = this.byId.get(c.id);
    if (old) { old.room = null; this.dirty.delete(old); this.byId.delete(c.id); old.close(4000); }   // cùng id nối lại: thay phiên cũ, không báo "rời"
    this.byId.set(c.id, c); c.room = this;
    const peers = []; for (const o of this.byId.values()) { if (o !== c && peers.length < 60) peers.push(o.info()); }
    c.send(JSON.stringify({ t: 'welcome', v: 2, room: this.name, max: MAX_ROOM, peers }));
    this.broadcast(JSON.stringify({ t: 'join', p: c.info() }), c);
  }
  remove(c) {
    if (this.byId.get(c.id) !== c) return;
    this.byId.delete(c.id); this.dirty.delete(c);
    this.broadcast(JSON.stringify({ t: 'leave', id: c.id }), c);
    if (!this.byId.size) rooms.delete(this.name);
  }
  broadcast(str, except, critical = true) { for (const o of this.byId.values()) if (o !== except) o.send(str, critical); }
}
function assignRoom(name, id) {
  if (name === 'chung') name = '';
  if (name) {
    let r = rooms.get(name); if (!r) { r = new Room(name); rooms.set(name, r); }
    return (r.size < MAX_ROOM || r.byId.has(id)) ? r : null;
  }
  for (let i = 1; i <= 200; i++) {                       // phòng chung tự tách khi đầy
    const nm = i === 1 ? 'chung' : 'chung-' + i;
    let r = rooms.get(nm); if (!r) { r = new Room(nm); rooms.set(nm, r); }
    if (r.size < MAX_ROOM || r.byId.has(id)) return r;
  }
  return null;
}

class Conn {
  constructor(sock, ip) {
    this.sock = sock; this.ip = ip; this.buf = Buffer.alloc(0); this.fragOp = 0; this.frag = []; this.fragLen = 0;
    this.lastRx = Date.now(); this.tokens = 60; this.tokT = Date.now(); this.drops = 0;
    this.id = ''; this.room = null; this.n = 'Khách'; this.r = 'lam'; this.h = 0; this.x = 0; this.z = 19; this.y = 0; this.a = 0; this.lastEv = 0;
    this.closed = false; this.cleaned = false;
    this.helloT = setTimeout(() => this.close(4001), 5000);       // phải gửi hello trong 5 giây
    sock.on('data', d => { try { this.onData(d); } catch (e) { this.close(1011); } });
    sock.on('close', () => { this.closed = true; this.cleanup(); });
    sock.on('error', () => { });
  }
  info() { return { id: this.id, n: this.n, r: this.r, h: this.h, x: this.x, z: this.z, y: this.y, a: this.a }; }

  /* --- đọc khung WebSocket (RFC 6455), client luôn che mặt nạ --- */
  onData(d) {
    this.lastRx = Date.now();
    this.buf = this.buf.length ? Buffer.concat([this.buf, d]) : d;
    for (;;) {
      const b = this.buf; if (b.length < 2) break;
      const fin = !!(b[0] & 0x80), op = b[0] & 0x0f, masked = !!(b[1] & 0x80); let len = b[1] & 0x7f, off = 2;
      if (b[0] & 0x70) return this.close(1002);
      if (!masked) return this.close(1002);
      if (len === 126) { if (b.length < 4) break; len = b.readUInt16BE(2); off = 4; }
      else if (len === 127) { if (b.length < 10) break; if (b.readUInt32BE(2) !== 0) return this.close(1009); len = b.readUInt32BE(6); off = 10; }
      if (len > MAX_MSG) return this.close(1009);
      if (b.length < off + 4 + len) break;
      const mask = b.subarray(off, off + 4), p = Buffer.allocUnsafe(len);
      for (let i = 0; i < len; i++) p[i] = b[off + 4 + i] ^ mask[i & 3];
      this.buf = b.subarray(off + 4 + len);
      this.frame(fin, op, p);
      if (this.closed) return;
    }
    if (this.buf.length > MAX_MSG + 16) this.close(1009);
  }
  frame(fin, op, p) {
    if (op >= 8) {
      if (!fin || p.length > 125) return this.close(1002);
      if (op === 8) return this.close(1000);
      if (op === 9) { this.raw(0x8a, p); }                          // ping -> pong
      return;
    }
    if (op === 0) {
      if (!this.fragOp) return this.close(1002);
      this.frag.push(p); this.fragLen += p.length; if (this.fragLen > MAX_MSG) return this.close(1009);
      if (fin) { const full = Buffer.concat(this.frag), o = this.fragOp; this.fragOp = 0; this.frag = []; this.fragLen = 0; this.message(o, full); }
      return;
    }
    if (op === 1 || op === 2) {
      if (this.fragOp) return this.close(1002);
      if (fin) this.message(op, p); else { this.fragOp = op; this.frag = [p]; this.fragLen = p.length; }
      return;
    }
    this.close(1002);
  }
  message(op, p) {
    if (op !== 1) return this.close(1003);                          // chỉ nhận văn bản
    const now = Date.now(); this.tokens = Math.min(60, this.tokens + (now - this.tokT) * 0.03); this.tokT = now;   // ~30 tin/giây, cho phép bùng 60
    if (this.tokens < 1) { if (++this.drops > 300) this.close(4008); return; }
    this.tokens--;
    let m; try { m = JSON.parse(p.toString('utf8')); } catch (e) { return; }
    if (!m || typeof m !== 'object') return;
    this.handle(m);
  }
  handle(m) {
    const t = m.t;
    if (!this.room) { if (t === 'hello') this.hello(m); return; }
    if (t === 's') { this.x = clampN(m.x, -27, 27); this.z = clampN(m.z, -34, 40); this.y = clampN(m.y, -7, 7); this.a = poseOf(m.a); this.room.dirty.add(this); }
    else if (t === 'ping') this.send(JSON.stringify({ t: 'pong', ts: Number(m.ts) || 0 }));
    else if (t === 'e') {
      if (m.k !== 'bell') return;
      const now = Date.now(); if (now - this.lastEv < 2500) return; this.lastEv = now;
      this.room.broadcast(JSON.stringify({ t: 'ev', id: this.id, k: 'bell' }), this, false);
    }
    else if (t === 'bye') this.close(1000);
  }
  hello(m) {
    if (typeof m.id !== 'string' || !ID_RE.test(m.id)) return this.close(4003);
    clearTimeout(this.helloT);
    this.id = m.id; this.n = cleanName(m.n); this.r = m.r === 'nau' ? 'nau' : 'lam'; this.h = hairOf(m.h);
    this.x = clampN(m.x, -27, 27); this.z = clampN(m.z, -34, 40); this.y = clampN(m.y, -7, 7); this.a = poseOf(m.a);
    const name = typeof m.room === 'string' && ROOM_RE.test(m.room.toLowerCase()) ? m.room.toLowerCase() : '';
    const room = assignRoom(name, this.id);
    if (!room) { this.send(JSON.stringify({ t: 'full' })); return this.close(4002); }
    room.add(this);
  }

  /* --- gửi --- */
  send(str, critical = true) {
    if (this.closed) return false; const s = this.sock;
    if (s.destroyed || !s.writable) return false;
    if (!critical && s.writableLength > 32768) { this.drops++; return false; }     // mạng chậm: bỏ bớt gói trạng thái (gói sau sẽ thay thế)
    const b = Buffer.from(str), n = b.length; let h;
    if (n < 126) h = Buffer.from([0x81, n]);
    else if (n < 65536) { h = Buffer.alloc(4); h[0] = 0x81; h[1] = 126; h.writeUInt16BE(n, 2); }
    else { h = Buffer.alloc(10); h[0] = 0x81; h[1] = 127; h.writeUInt32BE(0, 2); h.writeUInt32BE(n, 6); }
    s.write(Buffer.concat([h, b]));
    if (s.writableLength > (1 << 20)) this.close(1013);
    return true;
  }
  raw(b0, payload) { try { this.sock.write(Buffer.concat([Buffer.from([b0, payload.length]), payload])); } catch (e) { } }
  close(code = 1000) {
    if (this.closed) return; this.closed = true;
    const b = Buffer.alloc(4); b[0] = 0x88; b[1] = 2; b.writeUInt16BE(code, 2);
    try { this.sock.end(b); } catch (e) { }
    setTimeout(() => { try { this.sock.destroy(); } catch (e) { } }, 1000).unref();
    this.cleanup();
  }
  cleanup() {
    if (this.cleaned) return; this.cleaned = true; clearTimeout(this.helloT);
    clients.delete(this);
    const n = (perIp.get(this.ip) || 1) - 1; if (n <= 0) perIp.delete(this.ip); else perIp.set(this.ip, n);
    if (this.room) { const r = this.room; this.room = null; r.remove(this); }
  }
}

/* ---------- 5. Vòng lặp gộp trạng thái + dọn kết nối chết ---------- */
setInterval(() => {
  for (const r of rooms.values()) {
    if (!r.dirty.size) continue;
    const list = [...r.dirty]; r.dirty.clear();
    const entries = list.map(c => [c.id, c.x, c.z, c.y, c.a]), movers = new Set(list);
    const all = JSON.stringify({ t: 'snap', d: entries });
    for (const c of r.byId.values()) {
      if (!movers.has(c)) { c.send(all, false); continue; }
      if (list.length === 1) continue;                                // chỉ mình tôi di chuyển: khỏi gửi lại cho chính tôi
      c.send(JSON.stringify({ t: 'snap', d: entries.filter(e => e[0] !== c.id) }), false);
    }
  }
}, TICK_MS).unref();
setInterval(() => { const now = Date.now(); for (const c of [...clients]) if (now - c.lastRx > IDLE_MS) c.close(4004); }, 5000).unref();

/* ---------- 6. HTTP + nâng cấp WebSocket ---------- */
const server = http.createServer((req, res) => {
  const url = req.url.split('?')[0];
  res.setHeader('X-Content-Type-Options', 'nosniff'); res.setHeader('Referrer-Policy', 'no-referrer');
  if (url === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store', 'Access-Control-Allow-Origin': '*' });
    return res.end(JSON.stringify({ ok: true, v: 4, uptime: Math.round(process.uptime()), clients: clients.size, rooms: rooms.size, maxRoom: MAX_ROOM, rssMB: Math.round(process.memoryUsage().rss / 1048576) }));
  }
  if (url === '/favicon.ico') { res.writeHead(204); return res.end(); }
  const name = FILES[url];
  if (!name || (req.method !== 'GET' && req.method !== 'HEAD')) { res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' }); return res.end('Not found'); }
  const f = getFile(name);
  if (!f) { res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' }); return res.end('Thiếu file ' + name + ' cạnh server.js'); }
  const gz = /\bgzip\b/.test(req.headers['accept-encoding'] || ''), tag = gz ? f.etagGz : f.etag;
  const h = { 'Content-Type': f.type, 'ETag': tag, 'Vary': 'Accept-Encoding', 'Cache-Control': name === 'index.html' ? 'no-cache' : 'public, max-age=86400' };
  if (req.headers['if-none-match'] === tag) { res.writeHead(304, h); return res.end(); }
  const body = gz ? f.gz : f.raw; if (gz) h['Content-Encoding'] = 'gzip'; h['Content-Length'] = body.length;
  res.writeHead(200, h); res.end(req.method === 'HEAD' ? undefined : body);
});
server.on('upgrade', (req, socket, head) => {
  const deny = (code, msg) => { try { socket.end(`HTTP/1.1 ${code} ${msg}\r\nConnection: close\r\nContent-Length: 0\r\n\r\n`); } catch (e) { } };
  socket.on('error', () => { });
  if (req.url.split('?')[0] !== '/ws') return deny(404, 'Not Found');
  if (ORIGINS.length && !ORIGINS.includes(req.headers.origin || '')) return deny(403, 'Forbidden');
  const key = req.headers['sec-websocket-key'];
  if (!key || String(req.headers.upgrade || '').toLowerCase() !== 'websocket' || req.headers['sec-websocket-version'] !== '13') return deny(400, 'Bad Request');
  if (clients.size >= MAX_CLIENTS) return deny(503, 'Service Unavailable');
  let ip = socket.remoteAddress || '?';
  if (TRUST_PROXY) { const xf = String(req.headers['x-forwarded-for'] || '').split(',')[0].trim(); if (xf) ip = xf; }
  if ((perIp.get(ip) || 0) >= MAX_PER_IP) return deny(429, 'Too Many Requests');
  const accept = crypto.createHash('sha1').update(key + GUID).digest('base64');
  socket.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + accept + '\r\n\r\n');
  socket.setNoDelay(true); socket.setKeepAlive(true, 30000);
  perIp.set(ip, (perIp.get(ip) || 0) + 1);
  const c = new Conn(socket, ip); clients.add(c);
  if (head && head.length) { try { c.onData(head); } catch (e) { c.close(1011); } }
});
server.on('clientError', (e, s) => { try { s.destroy(); } catch (_) { } });

server.listen(PORT, HOST, () => {
  const a = server.address();
  log(`Chùa An Lạc relay v4 đang chạy tại http://${HOST === '0.0.0.0' ? 'localhost' : HOST}:${a.port}  (WebSocket: /ws, kiểm tra: /health)`);
  if (!fs.existsSync(path.join(ROOT, 'index.html'))) log('CẢNH BÁO: chưa thấy index.html trong', ROOT);
});
function shutdown() {
  log('Đang tắt…'); for (const c of [...clients]) c.close(1001);
  server.close(() => process.exit(0)); setTimeout(() => process.exit(0), 3000).unref();
}
process.on('SIGTERM', shutdown); process.on('SIGINT', shutdown);
process.on('uncaughtException', e => { console.error('uncaughtException', e); });
process.on('unhandledRejection', e => { console.error('unhandledRejection', e); });
module.exports = { server };
