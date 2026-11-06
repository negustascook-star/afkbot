'use strict';
const mineflayer = require('mineflayer');
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { SocksClient } = require('socks');

// ========== KONFIGŪRACIJA ==========
// Vienintelis būtinas aplinkos kintamasis: ACCESS_KEY (prieigos raktas į panelę).
const CONFIG = {
    webPort: Number(process.env.PORT) || 3000,
    accessKey: process.env.ACCESS_KEY || '',
    maxBots: Number(process.env.MAX_BOTS) || 10,
    joinGapMs: 5000,     // tarpas tarp botų prisijungimų
    reconnectMs: 4000,   // laukimas po kick / atsijungimo
    // Viešų SOCKS5 proxy sąrašai (galima pakeisti kintamuoju PROXY_LISTS, atskirti kableliais)
    proxySources: (process.env.PROXY_LISTS || [
        'https://raw.githubusercontent.com/TheSpeedX/SOCKS-List/master/socks5.txt',
        'https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/socks5.txt',
    ].join(',')).split(',').map(s => s.trim()).filter(Boolean),
    // 'ping' = proxy tikrinamas Minecraft status užklausa (patikimiau), 'connect' = tik TCP ryšys
    proxyTestMode: process.env.PROXY_TEST_MODE === 'connect' ? 'connect' : 'ping',
    defaults: {          // tik pradinės reikšmės panelės formoje
        host: process.env.MC_HOST || 'play.mcbegedis.lt',
        port: Number(process.env.MC_PORT) || 25565,
        version: process.env.MC_VERSION || '1.20.1',
    },
};

if (!CONFIG.accessKey) {
    CONFIG.accessKey = crypto.randomBytes(9).toString('base64url');
    console.warn(`⚠️ ACCESS_KEY nenustatytas. Laikinas prieigos raktas (pasikeis po restarto): ${CONFIG.accessKey}`);
}

const sleep = ms => new Promise(r => setTimeout(r, ms));

// ========== JOKIŲ JUDĖJIMO PAKETŲ ==========
// Botas stovi vietoje: fizika išjungta ir judėjimo paketai nesiunčiami.
// Leidžiame tik atsakymą į serverio teleportaciją (pvz. po /res tp).
// Keep-alive ir prisijungimo paketai yra privalomi, kitaip serveris išmestų botą.
const MOVE_PACKETS = new Set(['position', 'position_look', 'look', 'flying']);
function silenceMovement(bot) {
    bot.physicsEnabled = false;
    const client = bot._client;
    let allowUntil = 0;
    client.prependListener('position', () => { allowUntil = Date.now() + 1500; });
    const write = client.write.bind(client);
    client.write = (name, params) => {
        if (MOVE_PACKETS.has(name) && Date.now() > allowUntil) return;
        return write(name, params);
    };
}

// ========== PROXY BASEINAS ==========
// Automatiškai paima viešus SOCKS5 sąrašus, patikrina per juos pasiekiamą Minecraft serverį
// ir kiekvienam botui duoda skirtingą proxy. Tiesiogiai (be proxy) botas niekada nesijungia.
const pool = { candidates: [], verified: [], bad: new Map(), inUse: new Set(), lastFetch: 0, fetching: null };
const proxyKey = p => `${p.host}:${p.port}`;
const isBad = k => (pool.bad.get(k) || 0) > Date.now();
const releaseProxy = p => { if (p) pool.inUse.delete(proxyKey(p)); };
const markBad = p => { if (p) { pool.bad.set(proxyKey(p), Date.now() + 30 * 60 * 1000); pool.inUse.delete(proxyKey(p)); } };

function refreshPool() {
    if (pool.fetching) return pool.fetching;
    pool.fetching = (async () => {
        const seen = new Set();
        const out = [];
        for (const url of CONFIG.proxySources) {
            try {
                const res = await fetch(url, { signal: AbortSignal.timeout(20000) });
                if (!res.ok) throw new Error(`HTTP ${res.status}`);
                for (const line of (await res.text()).split(/\r?\n/)) {
                    const m = line.trim().replace(/^socks5:\/\//i, '').match(/^(\d{1,3}(?:\.\d{1,3}){3}):(\d{2,5})$/);
                    if (!m || Number(m[2]) > 65535 || seen.has(`${m[1]}:${m[2]}`)) continue;
                    seen.add(`${m[1]}:${m[2]}`);
                    out.push({ host: m[1], port: Number(m[2]) });
                }
            } catch (e) {
                console.error(`Proxy sąrašo atsisiuntimas nepavyko (${url}): ${e.message}`);
            }
        }
        for (let i = out.length - 1; i > 0; i--) {
            const j = Math.floor(Math.random() * (i + 1));
            [out[i], out[j]] = [out[j], out[i]];
        }
        pool.candidates = out.slice(0, 5000);
        pool.lastFetch = Date.now();
        console.log(`🌐 Proxy sąrašas atnaujintas: ${pool.candidates.length} kandidatų`);
    })().finally(() => { pool.fetching = null; });
    return pool.fetching;
}

function varint(n) {
    const b = [];
    do { let t = n & 0x7f; n >>>= 7; if (n) t |= 0x80; b.push(t); } while (n);
    return Buffer.from(b);
}
function handshakePacket(host, port) {
    const h = Buffer.from(host, 'utf8');
    const data = Buffer.concat([varint(0), varint(763), varint(h.length), h, Buffer.from([port >> 8, port & 255]), varint(1)]);
    return Buffer.concat([varint(data.length), data]);
}

// Ar proxy tikrai perleidžia Minecraft srautą iki serverio?
function testProxy(p, dest) {
    return new Promise(resolve => {
        let done = false, sock = null;
        const finish = ok => { if (done) return; done = true; clearTimeout(timer); try { sock?.destroy(); } catch (_) { } resolve(ok); };
        const timer = setTimeout(() => finish(false), 8000);
        SocksClient.createConnection({ proxy: { host: p.host, port: p.port, type: 5 }, command: 'connect', destination: dest, timeout: 6000 })
            .then(info => {
                sock = info.socket;
                if (done) return sock.destroy();
                if (CONFIG.proxyTestMode === 'connect') return finish(true);
                sock.on('error', () => finish(false));
                sock.once('data', d => finish(d.length > 2));
                sock.write(Buffer.concat([handshakePacket(dest.host, dest.port), Buffer.from([1, 0])]));
            })
            .catch(() => finish(false));
    });
}

async function acquireProxy(worker, dest, maxMs = 90000) {
    const destKey = `${dest.host}:${dest.port}`;
    const deadline = Date.now() + maxMs;
    while (Date.now() < deadline) {
        if (worker.removed || !worker.wantOnline) return null;

        // 1) jau patikrintas ir laisvas proxy (patikrinimas galioja 5 min.)
        pool.verified = pool.verified.filter(v => Date.now() - v.at < 5 * 60 * 1000);
        const vi = pool.verified.findIndex(v => v.destKey === destKey && !pool.inUse.has(proxyKey(v.p)));
        if (vi >= 0) {
            const p = pool.verified.splice(vi, 1)[0].p;
            pool.inUse.add(proxyKey(p));
            return p;
        }

        // 2) reikia daugiau kandidatų
        const age = Date.now() - pool.lastFetch;
        if (age > 30 * 60 * 1000 || (!pool.candidates.length && age > 60 * 1000)) await refreshPool();
        if (!pool.candidates.length) { await sleep(3000); continue; }

        // 3) tikriname paketą po 20 lygiagrečiai
        const batch = [];
        while (batch.length < 20 && pool.candidates.length) {
            const p = pool.candidates.pop();
            if (!pool.inUse.has(proxyKey(p)) && !isBad(proxyKey(p))) batch.push(p);
        }
        const results = await Promise.all(batch.map(async p => (await testProxy(p, dest)) ? p : null));
        for (const p of results) if (p) pool.verified.push({ p, destKey, at: Date.now() });
    }
    return null;
}

// ========== PRISIJUNGIMŲ EILĖ (po vieną, 5 s tarpais) ==========
let joinQueue = Promise.resolve();
function enqueueJoin(worker) {
    joinQueue = joinQueue.then(async () => {
        worker.queued = false;
        if (worker.removed || !worker.wantOnline || worker.bot) return;
        worker.connect();
        await sleep(CONFIG.joinGapMs);
    }).catch(e => console.error('Eilės klaida:', e));
}

// ========== BOTAS ==========
class Worker {
    constructor(cfg) {
        this.cfg = cfg;
        this.status = 'offline'; // offline | queued | connecting | online | reconnecting
        this.wantOnline = cfg.enabled;
        this.removed = false;
        this.queued = false;
        this.bot = null;
        this.runId = 0;
        this.logs = [];
        this.lastMsg = '';
        this.reconnectTimer = null;
        this.reloginTimer = null;
        this.connectTimer = null;
        this.nextReloginAt = 0;
        this.startedAt = null;
        this.listenUntil = 0;
        this.proxy = null;
        this.spawned = false;
        this.chat = [];      // konsolė: serverio žinutės, įvykiai ir tavo komandos
        this.chatSeq = 0;
    }

    log(msg, dedupe = false) {
        if (dedupe && this.lastMsg === msg) return;
        this.lastMsg = msg;
        const time = new Date().toLocaleTimeString('lt-LT', { hour12: false, timeZone: 'Europe/Vilnius' });
        this.logs.push(`${time} ${msg}`);
        if (this.logs.length > 80) this.logs.shift();
        this.pushChat(msg, 'sys');
        console.log(`[${this.cfg.name}] ${msg}`);
    }

    // kind: srv = serverio žinutė, sys = boto įvykis, me = tavo išsiųsta komanda
    pushChat(text, kind) {
        const t = new Date().toLocaleTimeString('lt-LT', { hour12: false, timeZone: 'Europe/Vilnius' });
        this.chat.push({ n: ++this.chatSeq, t, text, kind });
        if (this.chat.length > 300) this.chat.shift();
    }

    say(text) {
        if (!this.bot || this.status !== 'online') throw new Error('Botas nėra prisijungęs');
        text = String(text || '').replace(/[\r\n]+/g, ' ').trim().slice(0, 256);
        if (!text) throw new Error('Tuščia žinutė');
        this.bot.chat(text);
        this.pushChat(text, 'me');
    }

    isCurrent(bot, run) { return this.bot === bot && this.runId === run && !this.removed; }

    stopTimers() {
        clearTimeout(this.reconnectTimer);
        clearTimeout(this.connectTimer);
        clearInterval(this.reloginTimer);
        this.nextReloginAt = 0;
    }

    dropProxy(bad) {
        if (!this.proxy) return;
        if (bad) markBad(this.proxy); else releaseProxy(this.proxy);
        this.proxy = null;
    }

    async findProxy() {
        this.log('🔎 Ieškau veikiančio proxy...');
        const p = await acquireProxy(this, { host: this.cfg.host, port: this.cfg.port });
        if (!p) return false;
        if (this.removed || !this.wantOnline) { releaseProxy(p); return false; }
        this.proxy = p;
        this.log(`🌐 Proxy rastas: ${proxyKey(p)}`);
        return true;
    }

    queueConnect() {
        if (this.queued || this.bot || this.removed || !this.wantOnline) return;
        this.queued = true;
        if (this.cfg.useProxy && !this.proxy) {
            this.status = 'proxy';
            this.findProxy().then(ok => {
                if (ok) return enqueueJoin(this);
                this.queued = false;
                if (this.wantOnline && !this.removed) {
                    this.log('❌ Veikiančio proxy nerasta, bandysiu po 15 s', true);
                    this.status = 'reconnecting';
                    clearTimeout(this.reconnectTimer);
                    this.reconnectTimer = setTimeout(() => this.queueConnect(), 15000);
                } else this.status = 'offline';
            });
        } else {
            this.status = 'queued';
            enqueueJoin(this);
        }
    }

    connect() {
        const c = this.cfg;
        if (c.useProxy && !this.proxy) { this.scheduleReconnect(); return; } // niekada nejungiamės tiesiogiai
        this.status = 'connecting';
        this.spawned = false;
        this.log(`🔌 Jungiuosi į ${c.host}:${c.port}${this.proxy ? ` per ${proxyKey(this.proxy)}` : ''}...`);
        const opts = { host: c.host, port: c.port, username: c.name };
        if (c.version) opts.version = c.version;
        let botRef = null;
        if (this.proxy) {
            const proxy = this.proxy;
            opts.connect = client => {
                SocksClient.createConnection({
                    proxy: { host: proxy.host, port: proxy.port, type: 5 },
                    command: 'connect',
                    destination: { host: c.host, port: c.port },
                    timeout: 15000,
                }).then(info => {
                    client.setSocket(info.socket);
                    client.emit('connect');
                }).catch(err => {
                    this.log(`⚠️ Proxy klaida: ${err.message}`);
                    if (botRef && this.bot === botRef) {
                        try { botRef.end(); } catch (_) { }
                        this.onEnd(botRef, 'proxy neprisijungė');
                    }
                });
            };
        }
        const bot = mineflayer.createBot(opts);
        botRef = bot;
        this.bot = bot;
        silenceMovement(bot);

        clearTimeout(this.connectTimer);
        this.connectTimer = setTimeout(() => {
            if (this.bot === bot && this.status === 'connecting') {
                this.log('⏱️ Prisijungimas užtruko per ilgai');
                try { bot.end(); } catch (_) { }
                this.onEnd(bot, 'timeout');
            }
        }, 45000);

        bot.on('error', err => {
            if (err.message && err.message.includes('PartialReadError')) return;
            this.log(`⚠️ Klaida: ${err.message}`);
        });
        bot.on('kicked', reason => {
            const text = typeof reason === 'string' ? reason : JSON.stringify(reason);
            this.log(`🚫 Išmestas: ${text.slice(0, 200)}`);
        });
        bot.on('message', m => {
            const text = m.toString().trim();
            if (text) this.pushChat(text.slice(0, 400), 'srv');
        });
        bot.once('spawn', () => this.onSpawn(bot));
        bot.on('end', reason => this.onEnd(bot, reason));
    }

    async onSpawn(bot) {
        if (this.bot !== bot) return;
        const run = ++this.runId;
        const c = this.cfg;
        clearTimeout(this.connectTimer);
        this.spawned = true;
        this.status = 'online';
        this.startedAt = Date.now();
        this.listenUntil = Date.now() + 15000;
        this.log('✅ Prisijungė, stovi AFK');
        try {
            if (c.password) {
                await sleep(2000);
                if (!this.isCurrent(bot, run)) return;
                if (c.register) {
                    bot.chat(c.registerConfirm ? `/register ${c.password} ${c.password}` : `/register ${c.password}`);
                    this.log('📝 Išsiųsta /register (toliau bus naudojamas /login)');
                    c.register = false;
                    saveBots();
                } else {
                    bot.chat(`/login ${c.password}`);
                    this.log('🔑 Išsiųsta /login');
                }
            }
            if (c.postCommand) {
                await sleep(2000);
                if (!this.isCurrent(bot, run)) return;
                bot.chat(c.postCommand);
                this.log(`➡️ Išsiųsta ${c.postCommand}`);
            }
            this.startRelogin(bot, run);
        } catch (e) {
            this.log(`⚠️ Klaida po prisijungimo: ${e.message}`);
        }
    }

    // Kas N valandų po kiekvieno prisijungimo ar restarto siunčia /login
    startRelogin(bot, run) {
        clearInterval(this.reloginTimer);
        this.nextReloginAt = 0;
        const hours = Number(this.cfg.reloginHours) || 0;
        if (!hours || !this.cfg.password) return;
        const ms = hours * 3600 * 1000;
        this.nextReloginAt = Date.now() + ms;
        this.reloginTimer = setInterval(() => {
            if (!this.isCurrent(bot, run)) return;
            bot.chat(`/login ${this.cfg.password}`);
            this.listenUntil = Date.now() + 10000;
            this.nextReloginAt = Date.now() + ms;
            this.log('🔑 Periodinis /login');
        }, ms);
    }

    onEnd(bot, reason) {
        if (this.bot !== bot) return;
        clearTimeout(this.connectTimer);
        if (this.proxy && !this.spawned) {
            this.log(`🗑️ Proxy ${proxyKey(this.proxy)} neveikia, imsiu kitą`);
            this.dropProxy(true);
        }
        this.bot = null;
        this.runId++;
        this.startedAt = null;
        this.stopTimers();
        this.status = 'offline';
        this.log(`🔌 Atsijungė${reason ? ` (${reason})` : ''}`);
        if (this.wantOnline && !this.removed) this.scheduleReconnect();
    }

    scheduleReconnect() {
        clearTimeout(this.reconnectTimer);
        this.status = 'reconnecting';
        this.log(`⏳ Jungsiuosi iš naujo po ${CONFIG.reconnectMs / 1000} s`);
        this.reconnectTimer = setTimeout(() => this.queueConnect(), CONFIG.reconnectMs);
    }

    restart() {
        if (!this.cfg.enabled) return;
        this.log('🔄 Perkraunamas');
        this.wantOnline = true;
        if (this.bot) {
            try { this.bot.quit(); } catch (_) { }   // 'end' iškvies reconnect
        } else {
            clearTimeout(this.reconnectTimer);
            this.queueConnect();
        }
    }

    start() {
        this.cfg.enabled = true;
        this.wantOnline = true;
        saveBots();
        this.log('▶️ Paleistas');
        if (!this.bot) { clearTimeout(this.reconnectTimer); this.queueConnect(); }
    }

    stop() {
        this.cfg.enabled = false;
        this.wantOnline = false;
        saveBots();
        this.stopTimers();
        this.dropProxy(false);
        this.log('⏹️ Sustabdytas');
        if (this.bot) { try { this.bot.quit(); } catch (_) { } }
        else this.status = 'offline';
    }

    update(newCfg) {
        const old = this.cfg;
        this.cfg = { ...newCfg, id: old.id, enabled: old.enabled };
        if (!this.cfg.useProxy || old.host !== this.cfg.host || old.port !== this.cfg.port) this.dropProxy(false);
        saveBots();
        this.log('⚙️ Nustatymai atnaujinti');
        this.restart();
    }

    remove() {
        this.removed = true;
        this.wantOnline = false;
        this.stopTimers();
        this.dropProxy(false);
        try { this.bot?.quit(); } catch (_) { }
    }

    toJSON() {
        return {
            id: this.cfg.id,
            config: this.cfg,
            status: this.status,
            uptimeMs: this.startedAt ? Date.now() - this.startedAt : 0,
            nextReloginAt: this.nextReloginAt,
            proxy: this.proxy ? proxyKey(this.proxy) : null,
            logs: this.logs.slice(-25),
        };
    }
}

// ========== BOTŲ SĄRAŠAS IR SAUGOJIMAS ==========
const workers = new Map(); // id -> Worker
const DATA_DIR = path.join(__dirname, 'data');
const BOTS_FILE = path.join(DATA_DIR, 'bots.json');

function saveBots() {
    try {
        fs.mkdirSync(DATA_DIR, { recursive: true });
        fs.writeFileSync(BOTS_FILE, JSON.stringify([...workers.values()].map(w => w.cfg), null, 2));
    } catch (e) {
        console.error('Nepavyko išsaugoti nustatymų:', e.message);
    }
}

function cleanConfig(b, existing) {
    const s = (v, max) => String(v ?? '').trim().slice(0, max);
    const cfg = {
        name: s(b.name, 16),
        host: s(b.host, 255),
        port: parseInt(b.port, 10) || 25565,
        version: s(b.version, 10),
        password: String(b.password ?? '').slice(0, 100),
        register: !!b.register,
        registerConfirm: !!b.registerConfirm,
        reloginHours: b.reloginHours === '' || b.reloginHours == null ? 5 : Number(b.reloginHours),
        postCommand: b.postCommand == null ? '/home' : s(b.postCommand, 100), // numatyta: /home
        useProxy: !!b.useProxy,
        enabled: existing ? existing.enabled !== false : true,
    };
    if (!/^\w{3,16}$/.test(cfg.name)) throw new Error('Vardas turi būti 3–16 simbolių (raidės, skaičiai, _)');
    if (!/^[\w.\-]+$/.test(cfg.host)) throw new Error('Neteisingas serverio adresas');
    if (cfg.port < 1 || cfg.port > 65535) throw new Error('Prievadas turi būti 1–65535');
    if (cfg.version && !/^\d+\.\d+(\.\d+)?$/.test(cfg.version)) throw new Error('Versija turi atrodyti taip: 1.20.1 (arba palik tuščią)');
    if (!Number.isFinite(cfg.reloginHours) || cfg.reloginHours < 0 || cfg.reloginHours > 168) throw new Error('Pakartotinis /login: 0–168 val. (0 išjungia)');
    if (/[\r\n]/.test(cfg.password) || /[\r\n]/.test(cfg.postCommand)) throw new Error('Slaptažodyje ir komandoje negali būti naujos eilutės');
    return cfg;
}

function assertUnique(cfg, exceptId) {
    const key = `${cfg.name}@${cfg.host}:${cfg.port}`.toLowerCase();
    for (const w of workers.values()) {
        if (w.cfg.id === exceptId) continue;
        if (`${w.cfg.name}@${w.cfg.host}:${w.cfg.port}`.toLowerCase() === key) throw new Error('Toks botas šiame serveryje jau yra');
    }
}

function addWorker(input, id) {
    if (workers.size >= CONFIG.maxBots) throw new Error(`Daugiausia ${CONFIG.maxBots} botų`);
    const cfg = cleanConfig(input, { enabled: input.enabled !== false });
    cfg.id = id || crypto.randomUUID().slice(0, 8);
    assertUnique(cfg);
    const w = new Worker(cfg);
    workers.set(cfg.id, w);
    saveBots();
    w.queueConnect();
    return w;
}

function loadConfigs() {
    try {
        const arr = JSON.parse(fs.readFileSync(BOTS_FILE, 'utf8'));
        if (Array.isArray(arr)) return arr;
    } catch (_) { }
    try {
        // Pradinis sąrašas Render aplinkos kintamajame BOTS_JSON (nes nemokamas diskas išsivalo)
        if (process.env.BOTS_JSON) {
            const arr = JSON.parse(process.env.BOTS_JSON);
            if (Array.isArray(arr)) return arr;
        }
    } catch (e) { console.error('BOTS_JSON klaida:', e.message); }
    return [];
}

// ========== AUTORIZACIJA ==========
const SESSION_TOKEN = crypto.createHmac('sha256', CONFIG.accessKey).update('panel-session-v1').digest('hex');

function safeEqual(a, b) {
    const ha = crypto.createHash('sha256').update(String(a)).digest();
    const hb = crypto.createHash('sha256').update(String(b)).digest();
    return crypto.timingSafeEqual(ha, hb);
}

function getCookie(req, name) {
    for (const part of (req.headers.cookie || '').split(';')) {
        const [k, ...v] = part.trim().split('=');
        if (k === name) return v.join('=');
    }
    return '';
}

const isAuthed = req => { const t = getCookie(req, 'panel_session'); return !!t && safeEqual(t, SESSION_TOKEN); };

const fails = new Map(); // ip -> { count, until }
const clientIp = req => String(req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').split(',')[0].trim();
const isBlocked = ip => (fails.get(ip)?.until || 0) > Date.now();
function registerFail(ip) {
    const f = fails.get(ip) || { count: 0, until: 0 };
    if (++f.count >= 5) { f.until = Date.now() + 10 * 60 * 1000; f.count = 0; }
    fails.set(ip, f);
}

function cookieHeader(req, value, maxAge) {
    const secure = req.headers['x-forwarded-proto'] === 'https' ? '; Secure' : '';
    return `panel_session=${value}; HttpOnly; SameSite=Strict; Path=/; Max-Age=${maxAge}${secure}`;
}

// ========== WEB SERVERIS ==========
const htmlPath = [path.join(__dirname, 'public', 'index.html'), path.join(__dirname, 'index.html')].find(p => fs.existsSync(p));
if (!htmlPath) {
    console.error('❌ Nerastas index.html. Įkelk jį į GitHub kaip public/index.html (arba šalia index.js).');
    process.exit(1);
}
const INDEX_HTML = fs.readFileSync(htmlPath);

function sendJson(res, code, obj, headers = {}) {
    res.writeHead(code, { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store', ...headers });
    res.end(JSON.stringify(obj));
}

function readBody(req) {
    return new Promise(resolve => {
        let data = '';
        req.on('data', c => { data += c; if (data.length > 20000) req.destroy(); });
        req.on('end', () => { try { resolve(JSON.parse(data || '{}')); } catch (_) { resolve({}); } });
    });
}

const server = http.createServer(async (req, res) => {
    try {
        const urlObj = new URL(req.url, 'http://localhost');
        const p = urlObj.pathname;

        // UptimeRobot pingas (be rakto)
        if (p === '/health') {
            res.writeHead(200, { 'Content-Type': 'text/plain' });
            return res.end('ok');
        }

        // Pats puslapis nieko slapto neturi: rakto langą rodo pats puslapis
        if (req.method === 'GET' && p === '/') {
            res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-store' });
            return res.end(INDEX_HTML);
        }

        if (req.method === 'POST' && p === '/api/auth') {
            const ip = clientIp(req);
            if (isBlocked(ip)) return sendJson(res, 429, { error: 'Per daug bandymų. Palaukite 10 min.' });
            const body = await readBody(req);
            if (!safeEqual(body.key ?? '', CONFIG.accessKey)) {
                registerFail(ip);
                return sendJson(res, 401, { error: 'Neteisingas prieigos raktas' });
            }
            fails.delete(ip);
            return sendJson(res, 200, { ok: true }, { 'Set-Cookie': cookieHeader(req, SESSION_TOKEN, 30 * 24 * 3600) });
        }

        if (req.method === 'POST' && p === '/api/logout') {
            return sendJson(res, 200, { ok: true }, { 'Set-Cookie': cookieHeader(req, '', 0) });
        }

        if (!p.startsWith('/api/')) return sendJson(res, 404, { error: 'Nerasta' });
        if (!isAuthed(req)) return sendJson(res, 401, { error: 'Reikalingas prieigos raktas' });

        if (req.method === 'GET' && p === '/api/state') {
            return sendJson(res, 200, {
                bots: [...workers.values()].map(w => w.toJSON()),
                defaults: CONFIG.defaults,
                joinGapSec: CONFIG.joinGapMs / 1000,
                reconnectSec: CONFIG.reconnectMs / 1000,
                maxBots: CONFIG.maxBots,
                proxyPool: {
                    candidates: pool.candidates.length,
                    verified: pool.verified.length,
                    inUse: pool.inUse.size,
                    bad: [...pool.bad.values()].filter(t => t > Date.now()).length,
                },
            });
        }

        if (req.method === 'GET' && p === '/api/export') {
            res.writeHead(200, { 'Content-Type': 'application/json', 'Content-Disposition': 'attachment; filename="bots.json"' });
            return res.end(JSON.stringify([...workers.values()].map(w => w.cfg)));
        }

        if (req.method === 'POST' && p === '/api/bots') {
            try { addWorker(await readBody(req)); return sendJson(res, 200, { ok: true }); }
            catch (e) { return sendJson(res, 400, { error: e.message }); }
        }

        const m = p.match(/^\/api\/bots\/([\w-]+)(?:\/([a-z]+))?$/);
        if (m) {
            const w = workers.get(m[1]);
            if (!w) return sendJson(res, 404, { error: 'Botas nerastas' });
            const action = m[2];

            if (req.method === 'PUT' && !action) {
                try {
                    const cfg = cleanConfig(await readBody(req), w.cfg);
                    assertUnique(cfg, w.cfg.id);
                    w.update(cfg);
                    return sendJson(res, 200, { ok: true });
                } catch (e) { return sendJson(res, 400, { error: e.message }); }
            }
            if (req.method === 'DELETE' && !action) {
                w.remove();
                workers.delete(w.cfg.id);
                saveBots();
                return sendJson(res, 200, { ok: true });
            }
            if (req.method === 'GET' && action === 'console') {
                const since = Number(urlObj.searchParams.get('since')) || 0;
                return sendJson(res, 200, { status: w.status, last: w.chatSeq, lines: w.chat.filter(l => l.n > since) });
            }
            if (req.method === 'POST' && action === 'say') {
                try { w.say((await readBody(req)).text); return sendJson(res, 200, { ok: true }); }
                catch (e) { return sendJson(res, 400, { error: e.message }); }
            }
            if (req.method === 'POST' && action === 'restart') { w.restart(); return sendJson(res, 200, { ok: true }); }
            if (req.method === 'POST' && action === 'start') { w.start(); return sendJson(res, 200, { ok: true }); }
            if (req.method === 'POST' && action === 'stop') { w.stop(); return sendJson(res, 200, { ok: true }); }
        }

        sendJson(res, 404, { error: 'Nerasta' });
    } catch (e) {
        sendJson(res, 500, { error: e.message });
    }
});

// ========== PALEIDIMAS ==========
process.on('unhandledRejection', e => console.error('unhandledRejection:', e));
process.on('uncaughtException', e => console.error('uncaughtException:', e));

function shutdown() {
    console.log('Stabdau botus...');
    for (const w of workers.values()) {
        w.wantOnline = false;
        w.stopTimers();
        try { w.bot?.quit(); } catch (_) { }
    }
    setTimeout(() => process.exit(0), 1500);
}
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);

server.listen(CONFIG.webPort, () => {
    console.log(`🌐 Panelė: http://localhost:${CONFIG.webPort}  (ping adresas: /health)`);
    for (const item of loadConfigs()) {
        try { addWorker(item, item.id); } catch (e) { console.error(`Botas ${item?.name}:`, e.message); }
    }
});
