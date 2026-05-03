const express = require('express');
const cors = require('cors');
const { fetchFromRadar, fetchFlight } = require('flightradar24-client');

const app = express();
const PORT = process.env.PORT || 3001;

const ALLOWED_ORIGIN = process.env.ALLOWED_ORIGIN || '*';
app.use(cors({ origin: ALLOWED_ORIGIN }));

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

const APPROACH_INTERVAL = 60000;     

// Contiguous Approach Zones (5 Massive Zones เพื่อไม่ให้ตกหล่นแม้แต่ลำเดียว)
const APPROACH_ZONES = [
    { name: 'HKT-Approach-Center', north: 10.0, west: 96.0, south: 6.0, east: 100.0, options: {} },
    { name: 'HKT-Approach-North', north: 35.0, west: 85.0, south: 10.0, east: 125.0, options: {} },
    { name: 'HKT-Approach-South', north: 6.0, west: 90.0, south: -15.0, east: 125.0, options: {} },
    { name: 'HKT-Approach-East', north: 10.0, west: 100.0, south: 6.0, east: 125.0, options: {} },
    { name: 'HKT-Approach-West', north: 10.0, west: 50.0, south: 6.0, east: 96.0, options: {} },
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
        const detailPromises = [];

        let requestIndex = 0;

        for (const flight of allFlights) {
            const destination = (flight.destination || "").toUpperCase();
            
            // กรอง 1: เอาเฉพาะไฟลท์ที่เป้าหมายคือ HKT
            if (destination !== "HKT") continue;

            // กรอง 2: เอาเฉพาะไฟลท์ที่บินอยู่บนฟ้า (altitude > 1500 ฟุต และไม่ได้อยู่บนพื้น)
            const altitude = flight.altitude ?? 0;
            if (flight.isOnGround || altitude <= 1500) continue;

            // จัดการ IATA Code: ดึง flight.flight ก่อน (เช่น PG255) ถ้าไม่มีถึงจะดึง ICAO (BKP255)
            const flightCode = flight.flight || flight.callsign || flight.registration || 'UNKNOWN';
            const normFlightCode = normalizeFlightNumber(flightCode);

            // The Smart Caching (1-Time Fetch) + 30 Mins TTL
            // เช็คว่าไฟลท์นี้เคยดึง ETA มาแล้วหรือยัง
            const nowMs = Date.now();
            if (trackedETAs.has(flight.id)) {
                const cachedData = trackedETAs.get(flight.id);
                // ถ้ามี ETA แล้ว และอายุไม่เกิน 30 นาที -> ใช้ของเดิม
                // แต่ถ้า ETA เป็น null (เคยดึงล้มเหลว) จะรอแค่ 2 นาทีแล้วให้ลองดึงใหม่
                const isFresh = cachedData.eta ? (nowMs - cachedData.fetchedAt < 30 * 60 * 1000) : (nowMs - cachedData.fetchedAt < 2 * 60 * 1000);
                
                if (isFresh) {
                    responseData.set(flight.id, {
                        Flight: normFlightCode,
                        ETA: cachedData.eta
                    });
                    continue; // ข้ามการดึง API ไปเลย
                }
            }

            // ถ้าเป็นไฟลท์ใหม่เพิ่งเข้าโซนมา ถึงจะเอาไปต่อคิวดึงข้อมูล
            const delay = requestIndex * 300; 
            requestIndex++;

            detailPromises.push((async () => {
                try {
                    await new Promise(resolve => setTimeout(resolve, delay));
                    const detail = await withTimeout(fetchFlight(flight.id), 30000, `ArrivalDetail-${flightCode}`);
                    const etaTime = detail.arrival || detail.scheduledArrival || null;
                    const hktEta = getHktTime(etaTime);
                    
                    if (hktEta) trackedETAs.set(flight.id, { eta: hktEta, fetchedAt: Date.now() });

                    responseData.set(flight.id, {
                        Flight: normFlightCode,
                        ETA: hktEta || null
                    });
                } catch (e) {
                    console.error(`  ⚠️ Detail fetch failed for ${normFlightCode}: ${e.message}`);
                    
                    // ป้องกันการ Spam: ถ้าล้มเหลว เซฟ null ไว้ และรอ 2 นาทีค่อยลองใหม่ (แก้บั๊กที่ทำให้ขึ้น null ยาวๆ 30 นาที)
                    const oldEta = trackedETAs.has(flight.id) ? trackedETAs.get(flight.id).eta : null;
                    trackedETAs.set(flight.id, { eta: oldEta, fetchedAt: Date.now() });

                    responseData.set(flight.id, {
                        Flight: normFlightCode,
                        ETA: oldEta 
                    });
                }
            })());
        }
        
        // Wait for all ETA detail requests to finish
        if (detailPromises.length > 0) {
            await Promise.all(detailPromises);
        }

        flightDataCache = Array.from(responseData.values());
        lastFetchTime = new Date();

    } finally { 
        releaseLock(); 
    }
}

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
