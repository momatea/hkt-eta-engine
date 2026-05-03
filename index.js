const express = require('express');
const cors = require('cors');
const { fetchFromRadar, fetchFlight } = require('flightradar24-client');

const app = express();
const PORT = process.env.PORT || 3001;

const ALLOWED_ORIGIN = process.env.ALLOWED_ORIGIN || '*';
app.use(cors({ origin: ALLOWED_ORIGIN }));

// Middleware: API Key Security
app.use((req, res, next) => {
    // ปล่อยผ่าน Health Check ให้ Render เช็คสถานะได้
    if (req.path === '/api/health') return next();

    // รับ API Key จาก Header (x-api-key) หรือต่อท้าย URL (?apikey=...)
    const providedKey = req.headers['x-api-key'] || req.query.apikey;
    const SECRET_KEY = process.env.API_SECRET || 'HKT-ETA-SECRET-999';

    if (providedKey !== SECRET_KEY) {
        return res.status(401).json({ error: 'Unauthorized: Invalid API Key' });
    }
    next();
});

// Rate Limiting (30 requests/min per IP)
const rateLimitMap = new Map();
const RATE_LIMIT_MAX = 30;
const RATE_LIMIT_WINDOW = 60000;
setInterval(() => rateLimitMap.clear(), RATE_LIMIT_WINDOW);

app.use((req, res, next) => {
    const ip = req.ip || req.connection.remoteAddress || 'unknown';
    const count = (rateLimitMap.get(ip) || 0) + 1;
    rateLimitMap.set(ip, count);
    if (count > RATE_LIMIT_MAX) {
        return res.status(429).json({ error: 'Too many requests. Try again in 1 minute.' });
    }
    next();
});

// Crash Guard
process.on('uncaughtException', (err) => {
    console.error(`[${new Date().toISOString()}] 💀 UNCAUGHT EXCEPTION: ${err.message}`);
});
process.on('unhandledRejection', (reason) => {
    console.error(`[${new Date().toISOString()}] 💀 UNHANDLED REJECTION: ${reason}`);
});

/**
 * API Timeout Wrapper: Prevents engine from hanging on slow FR24 responses
 */
async function withTimeout(promise, ms = 30000, label = 'API') {
    const timeout = new Promise((_, reject) => setTimeout(() => reject(new Error(`${label} Timeout`)), ms));
    return Promise.race([promise, timeout]);
}

/**
 * Helper: Strips leading zeros from numeric part of flight numbers/callsigns
 */
function normalizeFlightNumber(str) {
    if (!str) return '';
    return str.replace(/([A-Z]+)0+([1-9]\d*)/i, '$1$2').toUpperCase();
}

// Helper: Convert Server Timestamp (Unix ms) or Date to ISO +07:00
function getHktTime(input) {
    if (!input) return null;
    const date = (typeof input === 'number' && input < 2000000000) ? new Date(input * 1000) : new Date(input || Date.now());
    if (isNaN(date.getTime())) return null;
    const hkt = new Date(date.getTime() + (7 * 60 * 60 * 1000));
    return hkt.toISOString().replace(/\.\d{3}Z$/, "+07:00");
}

let flightDataCache = [];
let lastFetchTime = null;

// Cache สำหรับจำค่า ETA เก่า เผื่อโดน FR24 บล็อคชั่วคราว (นี่คือเทคนิคที่ระบบเก่าใช้ซ่อน Error ครับ)
const trackedETAs = new Map();
const etaFetchQueue = new Map(); // คิวการดึง ETA ช้าๆ (หนีแบน)

const APPROACH_INTERVAL = 60000;     

// Contiguous Approach Zones (ครอบคลุมครึ่งโลกตะวันออก + โซนซูมพื้นดิน)
const APPROACH_ZONES = [
    { name: 'HKT-Zone-Center', north: 15.0, west: 90.0, south: -10.0, east: 110.0, options: {} }, // ไทย, มาเลเซีย, สิงคโปร์
    { name: 'HKT-Zone-West', north: 50.0, west: 40.0, south: -10.0, east: 90.0, options: {} }, // ตะวันออกกลาง (Dubai/Qatar), อินเดีย
    { name: 'HKT-Zone-North', north: 65.0, west: 90.0, south: 15.0, east: 110.0, options: {} }, // จีนตอนกลาง, รัสเซีย, พม่า
    { name: 'HKT-Zone-East', north: 55.0, west: 110.0, south: 15.0, east: 150.0, options: {} }, // ญี่ปุ่น, เกาหลี, ไต้หวัน, จีน
    { name: 'HKT-Zone-South', north: 15.0, west: 110.0, south: -45.0, east: 160.0, options: {} } // ออสเตรเลีย, อินโดนีเซีย
];

const pollRunning = new Set(); 

async function pollGroup(zones, groupName) {
    if (pollRunning.has(groupName)) {
        console.log(`[${new Date().toISOString()}] ⏭️  Loop [${groupName}] skipped — previous poll still running`);
        return;
    }
    pollRunning.add(groupName);
    try {
        const flightMap = new Map();
        
        for (const zone of zones) {
            try {
                const flights = await withTimeout(fetchFromRadar(zone.north, zone.west, zone.south, zone.east, null, zone.options), 30000, `Radar-${zone.name}`);
                for (const f of flights) {
                    flightMap.set(f.id, f);
                }
                await new Promise(resolve => setTimeout(resolve, 200)); 
            } catch (err) {
                console.error(`  ⚠️ ${zone.name} radar check failed: ${err.message}`);
            }
        }

        await processFlightData(Array.from(flightMap.values()), groupName);

        console.log(`[${new Date().toISOString()}] Loop [${groupName}] | Found: ${flightMap.size} | Cached HKT Arrivals: ${flightDataCache.length}`);
    } catch (error) {
        console.error(`[${new Date().toISOString()}] Loop [${groupName}] Fatal Error: ${error.message}`);
    } finally {
        pollRunning.delete(groupName);
    }
}

let processLock = Promise.resolve();

async function processFlightData(allFlights, groupName) {
    const ticket = processLock;
    let releaseLock;
    processLock = new Promise(resolve => { releaseLock = resolve; });
    await ticket;

    try {
        const responseData = new Map();
        const responseData = new Map();

        for (const flight of allFlights) {
            const destination = (flight.destination || "").toUpperCase();
            if (destination !== "HKT") continue;

            const flightCode = flight.flight || flight.callsign || flight.registration || 'UNKNOWN';
            const normFlightCode = normalizeFlightNumber(flightCode);
            const altitude = flight.altitude ?? 0;

            if (flight.isOnGround || altitude <= 1500) continue;

            const nowMs = Date.now();
            let currentEta = null;

            if (trackedETAs.has(flight.id)) {
                const cachedData = trackedETAs.get(flight.id);
                const isFresh = cachedData.eta ? (nowMs - cachedData.fetchedAt < 5 * 60 * 1000) : (nowMs - cachedData.fetchedAt < 2 * 60 * 1000);
                
                currentEta = cachedData.eta; // แสดงค่าเก่าไปก่อน
                
                if (!isFresh && !etaFetchQueue.has(flight.id)) {
                    etaFetchQueue.set(flight.id, normFlightCode); // เอาลงคิวถ้าหมดอายุ
                }
            } else {
                if (!etaFetchQueue.has(flight.id)) {
                    etaFetchQueue.set(flight.id, normFlightCode); // เครื่องใหม่ เอาลงคิว
                }
            }

            responseData.set(flight.id, {
                Flight: normFlightCode,
                ETA: currentEta
            });
        }

        flightDataCache = Array.from(responseData.values());
        lastFetchTime = new Date();

    } finally { 
        releaseLock(); 
    }
}
// Background Queue Worker: ดึงข้อมูลทีละ 1 ลำ ทุกๆ 12 วินาที (ชัวร์ว่าไม่เกิน 5 ครั้ง/นาที หนีแบน 100%)
setInterval(async () => {
    if (etaFetchQueue.size === 0) return;
    
    // ดึงคิวแรกออกมา
    const flightId = etaFetchQueue.keys().next().value;
    const flightCode = etaFetchQueue.get(flightId);
    etaFetchQueue.delete(flightId);
    
    try {
        const detail = await withTimeout(fetchFlight(flightId), 30000, `QueueFetch`);
        const etaTime = detail.arrival || detail.scheduledArrival || null;
        const hktEta = getHktTime(etaTime);
        
        trackedETAs.set(flightId, { eta: hktEta, fetchedAt: Date.now() });
        
        // อัปเดต Cache ปัจจุบันให้หน้าเว็บเห็นทันทีโดยไม่ต้องรอรอบ 1 นาที
        for (let item of flightDataCache) {
            if (item.Flight === flightCode) {
                item.ETA = hktEta || null;
                break;
            }
        }
    } catch (e) {
        console.error(`  ⚠️ Queue fetch failed for ${flightCode}: ${e.message}`);
        // ถ้าพลาด ให้เซฟค่าเดิมไปก่อนเพื่อรอ 2 นาทีค่อยเข้าคิวใหม่
        const oldEta = trackedETAs.has(flightId) ? trackedETAs.get(flightId).eta : null;
        trackedETAs.set(flightId, { eta: oldEta, fetchedAt: Date.now() }); 
    }
}, 12000);
setInterval(() => pollGroup(APPROACH_ZONES, 'APPROACH'), APPROACH_INTERVAL);

pollGroup(APPROACH_ZONES, 'APPROACH').catch(err => console.error(`[INIT] APPROACH poll failed: ${err.message}`));

app.get('/api/flights/eta', (req, res) => res.json(flightDataCache));

app.get('/api/health', (req, res) => res.json({ 
    status: 'ok', 
    version: 'v1.0 (ETA Engine for Line OA)',
    uptime: Math.floor(process.uptime()) + 's',
    cacheLength: flightDataCache.length, 
    lastFetchTime 
}));

const server = app.listen(PORT, () => {
    console.log(`\n=============================================`);
    console.log(`🛰️  HKT-ETA-Engine (Line OA)`);
    console.log(`🌐 Port ${PORT} | Approach Polling: ${APPROACH_INTERVAL / 1000}s`);
    console.log(`=============================================\n`);
});

// Graceful shutdown
function shutdown(signal) {
    console.log(`[${new Date().toISOString()}] 🛑 ${signal} received — shutting down gracefully`);
    server.close(() => {
        console.log(`[${new Date().toISOString()}] ✅ Server closed. Exiting.`);
        process.exit(0);
    });
    setTimeout(() => { console.error('Forced exit after 10s'); process.exit(1); }, 10000);
}
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT',  () => shutdown('SIGINT'));
