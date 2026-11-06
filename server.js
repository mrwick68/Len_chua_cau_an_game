#!/usr/bin/env node
'use strict';
/* =====================================================================
   LÊN CHÙA CẦU AN — máy chủ trung chuyển (relay) · mod by mrwick68 · phiên bản 5.7
   - Không cần cài thư viện: chỉ cần Node.js >= 18.  Chạy:  node server.js
   - Một cổng duy nhất:
       GET  /              -> index.html (nén gzip, có ETag)
       GET  /three.module.min.js -> bản Three.js đặt cạnh server (tuỳ chọn)
       GET  /health        -> trạng thái JSON (số kết nối, số phòng, số người trên bảng xếp hạng)
       WS   /ws            -> kênh chơi online
   - Giao thức WebSocket (JSON):
       client -> {t:'hello',v:5,id,u,q,k,room,n,r,h,l,p,x,z,y,a}   (bắt buộc đầu tiên, trong 5 giây)
                 u = mã ẩn danh ngẫu nhiên của người chơi, h = kiểu tóc 0..4, l = bậc 0..17 (17 = Chân Tu), p = điểm bình an, a = dáng 0..17 (9 thiền định, 10 cúi chào, 11 ngồi ăn, 12 gõ đầu, 13 ký đầu, 14 cắn, 15 bê khay, 16 lần tràng hạt, 17 uống nước), r = màu áo: lam | nau (Phật tử) · dq | trang | vang (người tham quan), q = chế độ yên tĩnh 0/1, k = điểm cao nhất từng đạt (để giữ bậc khi điểm xếp hạng giảm)
       client -> {t:'s',x,z,y,a} | {t:'sc',l,p,k} | {t:'w',k,x} | {t:'lb'} | {t:'ia',to,k} | {t:'cq',to} | {t:'cr',to,ok} | {t:'cm',x} | {t:'cx'} | {t:'qt',v} | {t:'ping',ts} | {t:'e',k:'bell'} | {t:'bye'}
       server -> {t:'welcome',room,peers:[...],wishes:[[kind,text,mine,tuổi_giây],...]} | {t:'join',p} | {t:'leave',id}
                 {t:'w',k,x} (lời cầu an của người khác: k = ribbon|lantern, x = nội dung) | {t:'wa',ok:0,why} (lời cầu của bạn không được chia sẻ)
                 {t:'snap',d:[[id,x,z,y,a],...]}  (gộp, chỉ gửi phần thay đổi, mặc định 5 lần/giây)
                 {t:'lv',id,l,p} | {t:'lb',d:[[tên,bậc,điểm,đang_online],...],me,meRow,total}
                 {t:'fx',a,v,k} (tương tác: người a → người v, k = knock|thump|bite) | {t:'ia',ok:0,why} (bị từ chối) | {t:'q',id,q}
                 {t:'cq',from} (lời mời trò chuyện) | {t:'cw',to} (đã chuyển lời mời) | {t:'cr',from,ok} | {t:'cs',with} (bắt đầu) | {t:'cm',from,x} | {t:'cx',from,why} | {t:'ca',ok:0,why}
                 {t:'pong',ts} | {t:'ev',id,k} | {t:'full'}
   - Điểm xếp hạng giảm khi vắng mặt: quá 8 giờ không vào chùa → −10%, sau đó mỗi giờ vắng thêm → −2% nữa (tính trên số điểm lúc rời chùa),
     đến khi về 0 (sau 53 giờ). Người đang online không bị giảm. Bậc đã thăng được giữ nguyên.
   - Lời cầu an (≤ 120 ký tự, đã lọc từ thô tục, không kèm tên) chỉ nằm trong RAM: mỗi phòng giữ tối đa 10 lời cho dải lụa và 10 lời cho hoa đăng
     trong 6 giờ để người mới vào cũng thấy.
   - Trò chuyện: hai người mời – đồng ý để bắt đầu (đứng gần ≤ 3,4 m, ngoài chánh điện, không bật "yên tĩnh"). Tin nhắn ≤ 60 ký tự, đã lọc từ thô tục,
     của người đang trò chuyện được chuyển cho CẢ PHÒNG (hiện trên đầu người nói, mỗi người tối đa 3 tin gần nhất, tự tan sau 3–10 giây) và KHÔNG được lưu.
     Tự kết thúc cuộc trò chuyện khi đi xa quá 7 m, vào chánh điện, bật yên tĩnh hoặc một bên rời đi.
   - Vị trí và tên chỉ nằm trong RAM. Bảng xếp hạng (mã ẩn danh → tên, bậc, điểm) được lưu ra file board.json (thư mục DATA_DIR)
     để còn lại khi khởi động lại. Trên dịch vụ có đĩa tạm (ví dụ Render gói Free) file này mất khi dịch vụ khởi động lại;
     khi đó bảng sẽ được dựng lại dần từ điểm mà mỗi máy tự báo khi người chơi vào lại chùa.
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
const DATA_DIR = path.resolve(E.DATA_DIR || ROOT);     // nơi ghi board.json
const MAX_BOARD = num(E.MAX_BOARD, 5000, 10, 100000);  // số người tối đa lưu trên bảng xếp hạng
const MAX_MSG = 1024;                                  // byte tối đa mỗi tin nhắn từ client
const GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
const ID_RE = /^[a-z0-9]{4,16}$/, ROOM_RE = /^[a-z0-9-]{1,16}$/, UID_RE = /^[a-z0-9]{10,24}$/;
const BAD = new Set(['địt', 'đụ', 'đù', 'lồn', 'cặc', 'buồi', 'đéo', 'cứt', 'đĩ', 'dit', 'dm', 'đm', 'dmm', 'đmm', 'dcm', 'đcm', 'dkm', 'đkm', 'vcl', 'vkl', 'vl', 'clgt', 'cmm', 'fuck', 'fucking', 'shit', 'bitch', 'asshole', 'cunt', 'dick']);
const LVP = [0, 100, 200, 500, 1000, 1300, 1800, 2500, 5000, 12000, 18000, 25000, 30000, 40000, 80000, 100000, 500000, 990000];   // điểm tối thiểu của từng bậc (khớp với game; bậc cuối 17 = Chân Tu)
/* tương tác giữa người chơi: chỉ ngoài chánh điện, trong tầm gần, không gửi dồn dập (mana do mỗi máy tự tính) */
const HALL = { x0: -17.6, x1: 17.6, z0: -30.8, z1: -10.9 };         // chánh điện (khớp với game)
const inHall = (x, z) => x > HALL.x0 && x < HALL.x1 && z > HALL.z0 && z < HALL.z1;
const IA_KINDS = new Set(['knock', 'thump', 'bite']);
const ROBES = new Set(['lam', 'nau', 'dq', 'trang', 'vang']);         // màu áo hợp lệ
const CH_RANGE = 3.4, CH_END = 7, CH_MAX = 60, CH_TTL = num(E.CH_TTL_MS, 20000, 500, 120000), CH_REQ_GAP = num(E.CH_REQ_GAP_MS, 3000, 100, 60000), CH_MSG_GAP = 600;     // trò chuyện: mời trong 3,4 m, kết thúc khi xa quá 7 m
const IA_RANGE = 3.4, IA_GAP = 1200, HIT_WIN = 10000, HIT_MAX = 4;     // tầm tối đa 3,4 m; mỗi người chơi 1 lượt / 1,2 giây; một người bị tối đa 4 lượt / 10 giây
/* điểm xếp hạng giảm khi vắng mặt (khớp hệt với game) */
const decayPct = ms => { const h = ms / 3600e3; return h > 8 ? Math.min(100, 10 + 2 * Math.floor(h - 8)) : 0; };
const effP = (r, online) => online ? r.p : Math.floor(r.p * (100 - decayPct(Date.now() - r.t)) / 100);
/* lời cầu an dùng chung */
const WISH_MAX = 10, WISH_LEN = 120, WISH_GAP = num(E.WISH_GAP_MS, 6000, 100, 60000), WISH_TTL = num(E.WISH_TTL_MS, 6 * 3600e3, 1000, 7 * 86400e3);   // lời cầu giữ tối đa 10 lời / loại / phòng, trong 6 giờ
const lvOfP = p => { let l = 0; for (let i = 0; i < LVP.length; i++) if (p >= LVP[i]) l = i; return l; };

/* ---------- 2. Tiện ích ---------- */
const clampN = (v, a, b) => { const n = Number(v); return Number.isFinite(n) ? Math.round(Math.min(b, Math.max(a, n)) * 100) / 100 : 0; };
const poseOf = a => { const n = a | 0; return n >= 0 && n <= 17 ? n : 0; };
const hairOf = h => { const n = Math.round(Number(h)); return Number.isFinite(n) && n >= 0 && n <= 4 ? n : 0; };
const ptsOf = p => { const n = Math.round(Number(p)); return Number.isFinite(n) ? Math.min(1e6, Math.max(0, n)) : 0; };
const lvOf = (l, p, k = 0) => { const n = Math.round(Number(l)); return Math.min(Number.isFinite(n) ? Math.min(17, Math.max(0, n)) : 0, lvOfP(Math.max(p, k))); };   // bậc không thể cao hơn điểm cao nhất từng đạt
function cleanName(n) {
  let s = Array.from(String(n == null ? '' : n).replace(/[\u0000-\u001f\u007f<>]/g, '').replace(/\s+/g, ' ').trim()).slice(0, 16).join('');
  if (!s) return 'Khách';
  const toks = s.toLowerCase().split(/[^\p{L}\p{N}]+/u).filter(Boolean);
  return toks.some(t => BAD.has(t)) ? 'Khách' : s;
}
function cleanWish(x, max = WISH_LEN) {
  if (typeof x !== 'string') return '';                    // chỉ nhận chuỗi (không biến đối tượng / mảng thành chữ)
  const s = Array.from(String(x == null ? '' : x).replace(/[\u0000-\u001f\u007f<>]/g, ' ').replace(/\s+/g, ' ').trim()).slice(0, max).join('');
  if (!s) return '';
  const toks = s.toLowerCase().replace(/(.)\1{2,}/g, '$1').split(/[^\p{L}\p{N}]+/u).filter(Boolean);
  return toks.some(t => BAD.has(t)) ? '' : s;
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

/* ---------- 4. Bảng xếp hạng (mã ẩn danh → tên, bậc, điểm) ---------- */
const BOARD_FILE = path.join(DATA_DIR, 'board.json');
const board = new Map();                                 // uid -> {n,p,l,t}
let boardDirty = false, boardTimer = null;
try {
  const j = JSON.parse(fs.readFileSync(BOARD_FILE, 'utf8'));
  for (const r of Array.isArray(j) ? j : []) if (Array.isArray(r) && UID_RE.test(r[0])) board.set(r[0], { n: cleanName(r[1]), p: ptsOf(r[2]), l: lvOf(r[3], ptsOf(r[2]), ptsOf(r[5])), t: Number(r[4]) || 0, k: Math.max(ptsOf(r[5]), ptsOf(r[2])) });
} catch (e) { /* chưa có file: bảng trống */ }
function saveBoard() {
  boardTimer = null; if (!boardDirty) return; boardDirty = false;
  try { fs.mkdirSync(DATA_DIR, { recursive: true }); const tmp = BOARD_FILE + '.tmp'; fs.writeFileSync(tmp, JSON.stringify([...board].map(([u, r]) => [u, r.n, r.p, r.l, r.t, r.k]))); fs.renameSync(tmp, BOARD_FILE); } catch (e) { /* đĩa chỉ đọc: bỏ qua */ }
}
function boardSet(uid, name, p, l, k = 0) {              // p đã gộp với điểm đã giảm của lần trước (xem hello), nên ghi thẳng
  if (!UID_RE.test(uid || '')) return;
  const old = board.get(uid), np = ptsOf(p), nk = Math.max(ptsOf(k), np, old ? old.k || 0 : 0);
  board.set(uid, { n: name, p: np, l: Math.max(lvOf(l, np, nk), old ? old.l : 0), t: Date.now(), k: nk });
  if (board.size > MAX_BOARD) { let low = null; for (const [u, r] of board) { const e = effP(r, false); if (!low || e < low[2] || (e === low[2] && r.t < low[1].t)) low = [u, r, e]; } if (low) board.delete(low[0]); }
  boardDirty = true; if (!boardTimer) { boardTimer = setTimeout(saveBoard, 20000); boardTimer.unref(); }
}
function boardSeen(uid) {                                // rời chùa: ghi lại thời điểm cuối còn ở chùa (mốc bắt đầu tính vắng mặt)
  const r = board.get(uid); if (!r) return; r.t = Date.now(); boardDirty = true; if (!boardTimer) { boardTimer = setTimeout(saveBoard, 20000); boardTimer.unref(); }
}
function onlineUids() { const s = new Set(); for (const c of clients) if (c.u) s.add(c.u); return s; }
setInterval(() => {                                      // dọn người đã về 0 điểm và vắng hơn 7 ngày
  const now = Date.now(), on = onlineUids(); let n = 0;
  for (const [u, r] of board) if (!on.has(u) && now - r.t > 7 * 86400e3 && effP(r, false) === 0) { board.delete(u); n++; }
  if (n) { boardDirty = true; if (!boardTimer) { boardTimer = setTimeout(saveBoard, 20000); boardTimer.unref(); } }
  for (const [name, b] of wishStore) { b.rib = b.rib.filter(w => now - w.t < WISH_TTL); b.lan = b.lan.filter(w => now - w.t < WISH_TTL); if (!b.rib.length && !b.lan.length && now - b.t > WISH_TTL) wishStore.delete(name); }
}, num(E.PURGE_MS, 3600e3, 200, 86400e3)).unref();
/* ---------- 4b. Lời cầu an dùng chung (RAM) ---------- */
const wishStore = new Map();                             // tên phòng -> { rib: [{x,t,u}], lan: [{x,t,u}], t }
function wishBox(name) {
  let b = wishStore.get(name);
  if (!b) { if (wishStore.size >= 300) { let o = null; for (const [k, v] of wishStore) if (!o || v.t < o[1].t) o = [k, v]; if (o) wishStore.delete(o[0]); } b = { rib: [], lan: [], t: Date.now() }; wishStore.set(name, b); }
  return b;
}
function wishList(name, c) {                             // danh sách gửi cho người mới vào: [kind, text, của mình?, tuổi (giây)]
  const b = wishStore.get(name); if (!b) return [];
  const now = Date.now(), me = c.u || c.id, out = [];
  for (const [k, arr] of [['ribbon', b.rib], ['lantern', b.lan]]) for (const w of arr) if (now - w.t < WISH_TTL) out.push([k, w.x, w.u === me ? 1 : 0, Math.round((now - w.t) / 1000)]);
  return out;
}

/* ---------- 5. Phòng & kết nối ---------- */
const clients = new Set(), rooms = new Map(), perIp = new Map();
class Room {
  constructor(name) { this.name = name; this.byId = new Map(); this.dirty = new Set(); }
  get size() { return this.byId.size; }
  add(c) {
    const old = this.byId.get(c.id);
    if (old) { old.room = null; this.dirty.delete(old); this.byId.delete(c.id); old.close(4000); }   // cùng id nối lại: thay phiên cũ, không báo "rời"
    this.byId.set(c.id, c); c.room = this;
    const peers = []; for (const o of this.byId.values()) { if (o !== c && peers.length < 60) peers.push(o.info()); }
    c.send(JSON.stringify({ t: 'welcome', v: 4, room: this.name, max: MAX_ROOM, peers, wishes: wishList(this.name, c) }));
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
function leaderboardFor(uid) {                           // top 50 + vị trí của người hỏi; người vắng mặt bị giảm điểm theo thời gian vắng
  const online = onlineUids();
  const rows = [...board].map(([u, r]) => ({ u, r, e: effP(r, online.has(u)) })).sort((a, b) => b.e - a.e || b.r.t - a.r.t);
  const row = x => [x.r.n, x.r.l, x.e, online.has(x.u) ? 1 : 0];
  const d = rows.slice(0, 50).map(row);
  let me = 0, meRow = null; const i = rows.findIndex(x => x.u === uid);
  if (i >= 0) { me = i + 1; if (i >= 50) meRow = row(rows[i]); }
  return { t: 'lb', d, me, meRow, total: board.size };
}

class Conn {
  constructor(sock, ip) {
    this.sock = sock; this.ip = ip; this.buf = Buffer.alloc(0); this.fragOp = 0; this.frag = []; this.fragLen = 0;
    this.lastRx = Date.now(); this.tokens = 60; this.tokT = Date.now(); this.drops = 0;
    this.id = ''; this.u = ''; this.room = null; this.n = 'Khách'; this.r = 'lam'; this.h = 0; this.l = 0; this.p = 0; this.x = 0; this.z = 19; this.y = 0; this.a = 0; this.lastEv = 0; this.lastSc = 0; this.lastLb = 0; this.lastIa = 0; this.hits = []; this.q = 0; this.pk = 0; this.lastW = 0; this.chat = null; this.pend = null; this.incoming = null; this.lastCq = 0; this.lastCm = 0;
    this.closed = false; this.cleaned = false;
    this.helloT = setTimeout(() => this.close(4001), 5000);       // phải gửi hello trong 5 giây
    sock.on('data', d => { try { this.onData(d); } catch (e) { this.close(1011); } });
    sock.on('close', () => { this.closed = true; this.cleanup(); });
    sock.on('error', () => { });
  }
  info() { return { id: this.id, n: this.n, r: this.r, h: this.h, l: this.l, p: this.p, q: this.q, x: this.x, z: this.z, y: this.y, a: this.a }; }

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
    if (t === 's') { this.x = clampN(m.x, -60, 60); this.z = clampN(m.z, -34, 40); this.y = clampN(m.y, -7, 7); this.a = poseOf(m.a); this.room.dirty.add(this); if (this.chat) this.chatCheck(); }
    else if (t === 'ping') this.send(JSON.stringify({ t: 'pong', ts: Number(m.ts) || 0 }));
    else if (t === 'sc') {                                          // báo điểm / bậc mới (tối đa ~3 lần mỗi giây; máy khách tự gộp lại)
      const now = Date.now(); if (now - this.lastSc < 300) return; this.lastSc = now;
      this.p = Math.max(this.p, ptsOf(m.p)); this.pk = Math.max(this.pk, ptsOf(m.k), this.p); this.l = Math.max(this.l, lvOf(m.l, this.p, this.pk));
      boardSet(this.u, this.n, this.p, this.l, this.pk);
      this.room.broadcast(JSON.stringify({ t: 'lv', id: this.id, l: this.l, p: this.p }), this, false);
    }
    else if (t === 'qt') {                                          // bật / tắt chế độ yên tĩnh (không nhận và không gửi tương tác)
      const q = m.v ? 1 : 0; if (q === this.q) return; this.q = q; if (q) this.chatEnd('quiet');
      this.room.broadcast(JSON.stringify({ t: 'q', id: this.id, q }), this, false);
    }
    else if (t === 'ia') this.interact(m);
    else if (t === 'cq') this.chatReq(m);
    else if (t === 'cr') this.chatRes(m);
    else if (t === 'cm') this.chatMsg(m);
    else if (t === 'cx') this.chatEnd('bye');
    else if (t === 'w') this.wish(m);
    else if (t === 'lb') {                                          // xin bảng xếp hạng (tối đa 1 lần mỗi 2 giây)
      const now = Date.now(); if (now - this.lastLb < 2000) return; this.lastLb = now;
      this.send(JSON.stringify(leaderboardFor(this.u)));
    }
    else if (t === 'e') {
      if (m.k !== 'bell') return;
      const now = Date.now(); if (now - this.lastEv < 2500) return; this.lastEv = now;
      this.room.broadcast(JSON.stringify({ t: 'ev', id: this.id, k: 'bell' }), this, false);
    }
    else if (t === 'bye') this.close(1000);
  }
  wish(m) {                                                        // lời cầu an: lọc, lưu tạm, chuyển cho những người khác trong phòng
    const k = m.k === 'lantern' ? 'lantern' : m.k === 'ribbon' ? 'ribbon' : ''; if (!k) return;
    const now = Date.now(); if (now - this.lastW < WISH_GAP) return this.send(JSON.stringify({ t: 'wa', ok: 0, why: 'wait' }));
    const x = cleanWish(m.x); if (!x) return this.send(JSON.stringify({ t: 'wa', ok: 0, why: 'bad' }));
    this.lastW = now;
    const b = wishBox(this.room.name); b.t = now; const arr = k === 'ribbon' ? b.rib : b.lan;
    arr.push({ x, t: now, u: this.u || this.id }); while (arr.length > WISH_MAX) arr.shift();
    this.room.broadcast(JSON.stringify({ t: 'w', k, x }), this);
  }
  jsend(o) { return this.send(JSON.stringify(o)); }
  dist(v) { return Math.hypot(this.x - v.x, this.z - v.z); }
  chatReq(m) {                                                     // mời trò chuyện: kiểm tra rồi chuyển lời mời cho người kia
    const fail = why => this.jsend({ t: 'ca', ok: 0, why });
    const now = Date.now(); if (now - this.lastCq < CH_REQ_GAP) return fail('wait');
    const v = this.room.byId.get(typeof m.to === 'string' ? m.to : '');
    if (!v || v === this) return fail('none');
    if (this.q || v.q) return fail('quiet');
    if (this.chat || v.chat || v.incoming) return fail('busy');
    if (inHall(this.x, this.z) || inHall(v.x, v.z)) return fail('hall');
    if (this.dist(v) > CH_RANGE) return fail('far');
    this.cancelPend(); this.lastCq = now; this.pend = { to: v.id, t: now }; v.incoming = { from: this.id, t: now };
    v.jsend({ t: 'cq', from: this.id }); this.jsend({ t: 'cw', to: v.id });
  }
  cancelPend() {                                                   // huỷ lời mời đang chờ (của mình gửi đi)
    if (!this.pend) return; const v = this.room && this.room.byId.get(this.pend.to); this.pend = null;
    if (v && v.incoming && v.incoming.from === this.id) { v.incoming = null; v.jsend({ t: 'cx', from: this.id, why: 'cancel' }); }
  }
  chatRes(m) {                                                     // trả lời lời mời: ok = 1 đồng ý, 0 từ chối
    const v = this.room.byId.get(typeof m.to === 'string' ? m.to : '');
    if (!v || !this.incoming || this.incoming.from !== v.id || !v.pend || v.pend.to !== this.id) return;
    this.incoming = null; v.pend = null;
    if (!m.ok) return v.jsend({ t: 'cr', from: this.id, ok: 0 });
    if (this.q || v.q || this.chat || v.chat || inHall(this.x, this.z) || inHall(v.x, v.z) || this.dist(v) > CH_RANGE) return v.jsend({ t: 'cr', from: this.id, ok: 0 });
    this.chat = v; v.chat = this; this.jsend({ t: 'cs', with: v.id }); v.jsend({ t: 'cs', with: this.id });
  }
  chatMsg(m) {                                                     // tin nhắn: chỉ người đang trò chuyện mới gửi được, nhưng cả phòng đều nhận (trừ người gửi)
    if (!this.chat) return; const now = Date.now(); if (now - this.lastCm < CH_MSG_GAP) return;
    const x = cleanWish(m.x, CH_MAX); if (!x) return this.jsend({ t: 'ca', ok: 0, why: 'bad' });
    this.lastCm = now; this.room.broadcast(JSON.stringify({ t: 'cm', from: this.id, x }), this);
  }
  chatCheck() { const p = this.chat; if (p && (inHall(this.x, this.z) || inHall(p.x, p.z) || this.dist(p) > CH_END)) this.chatEnd('away'); }
  chatEnd(why) {                                                   // kết thúc trò chuyện / huỷ lời mời hai chiều
    const p = this.chat; if (p) { this.chat = null; p.chat = null; p.jsend({ t: 'cx', from: this.id, why }); this.jsend({ t: 'cx', from: p.id, why }); }
    this.cancelPend();
    if (this.incoming) { const r = this.room && this.room.byId.get(this.incoming.from); this.incoming = null; if (r) { r.pend = null; r.jsend({ t: 'cr', from: this.id, ok: 0 }); } }
  }
  interact(m) {                                                    // gõ đầu / ký đầu / cắn: kiểm tra rồi báo cho cả phòng (ai cũng thấy hiệu ứng)
    const fail = why => this.send(JSON.stringify({ t: 'ia', ok: 0, why }));
    const now = Date.now(); if (now - this.lastIa < IA_GAP) return fail('wait');
    const v = this.room.byId.get(typeof m.to === 'string' ? m.to : '');
    if (!IA_KINDS.has(m.k) || !v || v === this) return fail('none');
    if (this.q || v.q) return fail('quiet');
    if (inHall(this.x, this.z) || inHall(v.x, v.z)) return fail('hall');
    if (Math.hypot(this.x - v.x, this.z - v.z) > IA_RANGE) return fail('far');
    v.hits = v.hits.filter(t => now - t < HIT_WIN); if (v.hits.length >= HIT_MAX) return fail('busy');
    this.lastIa = now; v.hits.push(now);
    this.room.broadcast(JSON.stringify({ t: 'fx', a: this.id, v: v.id, k: m.k }));
  }
  hello(m) {
    if (typeof m.id !== 'string' || !ID_RE.test(m.id)) return this.close(4003);
    clearTimeout(this.helloT);
    this.id = m.id; this.n = cleanName(m.n); this.r = ROBES.has(m.r) ? m.r : 'lam'; this.h = hairOf(m.h); this.q = m.q ? 1 : 0;
    this.u = typeof m.u === 'string' && UID_RE.test(m.u) ? m.u : '';
    this.p = ptsOf(m.p); this.pk = Math.max(ptsOf(m.k), this.p); this.l = lvOf(m.l, this.p, this.pk);
    if (this.u) {
      const o = board.get(this.u);
      if (o) {                                                    // gộp với điểm ĐÃ GIẢM của lần trước (máy khác / đã vắng lâu), không lấy điểm gốc chưa giảm
        const oe = effP(o, [...clients].some(c => c !== this && c.u === this.u));      // đang online ở kết nối KHÁC thì không giảm; kết nối mới này không tính
        this.p = Math.max(this.p, oe); this.pk = Math.max(this.pk, o.k || 0, this.p); this.l = Math.max(this.l, o.l);
      }
      boardSet(this.u, this.n, this.p, this.l, this.pk);
    }
    this.x = clampN(m.x, -60, 60); this.z = clampN(m.z, -34, 40); this.y = clampN(m.y, -7, 7); this.a = poseOf(m.a);
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
    if (this.u) boardSeen(this.u);
    try { this.chatEnd('left'); } catch (e) { }
    if (this.room) { const r = this.room; this.room = null; r.remove(this); }
  }
}

/* ---------- 6. Vòng lặp gộp trạng thái + dọn kết nối chết ---------- */
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
setInterval(() => { const now = Date.now(); for (const c of [...clients]) { if (now - c.lastRx > IDLE_MS) c.close(4004); else if ((c.pend && now - c.pend.t > CH_TTL) || (c.incoming && now - c.incoming.t > CH_TTL)) { if (c.pend) c.cancelPend(); if (c.incoming) c.chatEnd('timeout'); } } }, 5000).unref();

/* ---------- 7. HTTP + nâng cấp WebSocket ---------- */
const server = http.createServer((req, res) => {
  const url = req.url.split('?')[0];
  res.setHeader('X-Content-Type-Options', 'nosniff'); res.setHeader('Referrer-Policy', 'no-referrer');
  if (url === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store', 'Access-Control-Allow-Origin': '*' });
    return res.end(JSON.stringify({ ok: true, v: 5.7, wishRooms: wishStore.size, uptime: Math.round(process.uptime()), clients: clients.size, rooms: rooms.size, board: board.size, maxRoom: MAX_ROOM, rssMB: Math.round(process.memoryUsage().rss / 1048576) }));
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
  log(`Chùa An Lạc relay v5.7 đang chạy tại http://${HOST === '0.0.0.0' ? 'localhost' : HOST}:${a.port}  (WebSocket: /ws, kiểm tra: /health) · bảng xếp hạng: ${board.size} người`);
  if (!fs.existsSync(path.join(ROOT, 'index.html'))) log('CẢNH BÁO: chưa thấy index.html trong', ROOT);
});
function shutdown() {
  log('Đang tắt…'); saveBoard(); for (const c of [...clients]) c.close(1001);
  server.close(() => process.exit(0)); setTimeout(() => process.exit(0), 3000).unref();
}
process.on('SIGTERM', shutdown); process.on('SIGINT', shutdown);
process.on('uncaughtException', e => { console.error('uncaughtException', e); });
process.on('unhandledRejection', e => { console.error('unhandledRejection', e); });
module.exports = { server };
