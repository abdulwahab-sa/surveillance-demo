require('dotenv').config();
const net = require('net');
const { WebSocketServer } = require('ws');
const mariadb = require('mariadb');
const path = require('path');
const express = require('express');
const http = require('http');
const fs = require('fs').promises;
const EventEmitter = require('events');

// --- CONFIG ---
const SOCKET_PORT = 9000;
const HTTP_PORT = 3005;
const BMP_FOLDER = path.resolve('./bmpData');
const DB_BATCH_SIZE = 30;
const DB_FLUSH_INTERVAL = 1500;
const STORAGE_QUEUE_MAX = 80;

// Playback Config
const PLAYBACK_BATCH_SIZE = 200;
const PLAYBACK_QUEUE_HIGH = 10;
const PLAYBACK_QUEUE_LOW = 3;
const PLAYBACK_DELAY_MS = 300;

// Logging utility
function log(message, level = 'INFO') {
	const timestamp = new Date().toTimeString().substring(0, 12);
	console.log(`[${timestamp}] [${level}] ${message}`);
}

log('Node.js surveillance server starting...');

// Create storage folder
fs.mkdir(BMP_FOLDER, { recursive: true })
	.then(() => log(`Storage folder: ${BMP_FOLDER}`))
	.catch((err) => log(`Storage folder error: ${err.message}`, 'ERROR'));

// --- Express + WebSocket Setup ---
const app = express();
app.use(express.json({ limit: '20mb' })); // for parsing application/json

// Serve static files from frontend directory
app.use(express.static(path.resolve(__dirname, '../frontend')));

// Route: Live feed page
app.get('/', (req, res) => {
	res.sendFile(path.join(__dirname, '../frontend/index.html'));
});

// Route: Playback page
app.get('/playback', (req, res) => {
	res.sendFile(path.join(__dirname, '../frontend/playback.html'));
});

const server = http.createServer(app);
const wss = new WebSocketServer({ server });

// --- MariaDB Connection Pool ---
const DB_HOST = 'localhost';
const DB_PORT = 3306;
const DB_USER = 'demo';
const DB_PASSWORD = 'demo123';
const DB_NAME = 'imgindex';

const pool = mariadb.createPool({
	host: DB_HOST,
	user: DB_USER,
	password: DB_PASSWORD,
	database: DB_NAME,
	connectionLimit: 5,
	port: DB_PORT,
	acquireTimeout: 20000,
});

log(`MariaDB pool created (${DB_HOST}/${DB_NAME})`);

// Test DB connection and validate index
pool
	.getConnection()
	.then(async (conn) => {
		log('Database connection: OK');

		// Check for timestamp index
		try {
			const indexes = await conn.query(`
				SHOW INDEX FROM tb_index 
				WHERE Column_name IN ('camNo', 't_year', 't_mon', 't_mday', 't_hour', 't_min', 't_sec')
			`);

			if (indexes.length === 0) {
				log('WARNING: No index found on timestamp fields! Playback queries will be slow.', 'WARN');
				log(
					'Recommended: CREATE INDEX idx_playback ON tb_index(camNo, t_year, t_mon, t_mday, t_hour, t_min, t_sec, t_mill);',
					'WARN'
				);
			} else {
				log('✓ Timestamp index exists');
			}
		} catch (err) {
			log(`Index check failed: ${err.message}`, 'WARN');
		}

		conn.end();
	})
	.catch((err) => log(`Database connection failed: ${err.message}`, 'ERROR'));

// --- WebSocket Connection Handling ---
let wsClientCount = 0;
// camNo -> session
const playbackSessions = new Map();

wss.on('connection', (ws) => {
	wsClientCount++;
	log(`WebSocket client connected (Total: ${wsClientCount})`);

	let activeSession = null;

	ws.on('message', async (message) => {
		try {
			const msg = JSON.parse(message.toString());

			// Start Playback Command
			if (msg.action === 'playback-start') {
				const { startTime, camNo, speed } = msg;

				if (!startTime || !startTime.year || !startTime.month || !startTime.day || !camNo) {
					ws.send(JSON.stringify({ type: 'error', message: 'Invalid playback parameters.' }));
					return;
				}

				// Stop any existing playback session for this camera
				if (playbackSessions.has(camNo)) {
					await stopPlayback(camNo);
				}

				// Stop any session this WebSocket was already running
				if (activeSession) {
					await stopPlayback(activeSession.camNo);
					activeSession = null;
				}

				// Create new playback session
				const sessionId = `${camNo}_${Date.now()}`;
				const session = {
					camNo,
					ws,
					startTime: new Date(
						startTime.year,
						startTime.month - 1,
						startTime.day,
						startTime.hour || 0,
						startTime.minute || 0,
						startTime.second || 0
					),
					speed: speed || 1.0,
					fileQueue: [],
					active: true,
					frameCount: 0,
					sessionId,
					workerRunning: false,
				};

				playbackSessions.set(camNo, session);
				activeSession = session;

				log(
					`[PLAYBACK] Starting session: ${camNo} from ${session.startTime.toISOString()} (speed: ${
						session.speed
					}x)`
				);

				ws.send(
					JSON.stringify({
						type: 'playback-started',
						camNo,
						startTime: session.startTime.toISOString(),
					})
				);

				startPlaybackSession(camNo);
				return;
			}

			//  Stop Playback Command
			if (msg.action === 'playback-stop') {
				if (activeSession) {
					await stopPlayback(activeSession.camNo);
					activeSession = null;
				}
				return;
			}

			//  Pause Playback
			if (msg.action === 'playback-pause') {
				if (activeSession) {
					activeSession.paused = true;
					log(`[PLAYBACK] Paused: ${activeSession.camNo}`);
				}
				return;
			}

			// Resume Playback
			if (msg.action === 'playback-resume') {
				if (activeSession) {
					activeSession.paused = false;
					log(`[PLAYBACK] Resumed: ${activeSession.camNo}`);
				}
				return;
			}

			// Unknown command
			log(`Unknown WebSocket command: ${msg.action || msg.type}`, 'WARN');
			ws.send(JSON.stringify({ type: 'error', message: 'Unknown command.' }));
		} catch (err) {
			log(`WebSocket message error: ${err.message}`, 'ERROR');
			ws.send(JSON.stringify({ type: 'error', message: 'Malformed message.' }));
		}
	});

	ws.on('close', () => {
		wsClientCount--;
		log(`WebSocket client disconnected (Remaining: ${wsClientCount})`);

		// Stop active playback session
		if (activeSession) {
			stopPlayback(activeSession.camNo);
			activeSession = null;
		}
	});

	ws.on('error', (err) => {
		log(`WebSocket error: ${err.message}`, 'ERROR');
	});
});

//  Broadcast Live Frame to All Clients
function broadcastFrameBinary(camNo, imageBuffer, timestamp) {
	if (wss.clients.size === 0) return;

	const header = JSON.stringify({ camNo, timestamp, type: 'live' });
	const headerBuffer = Buffer.from(header);
	const headerLength = Buffer.alloc(4);
	headerLength.writeUInt32BE(headerBuffer.length, 0);
	const payload = Buffer.concat([headerLength, headerBuffer, imageBuffer]);

	wss.clients.forEach((client) => {
		if (client.readyState === 1) {
			client.send(payload);
		}
	});
}

//  Event-Driven DB Insert Queue
const dbEvents = new EventEmitter();
let dbInsertQueue = [];
let dbFlushTimer = null;
let totalDbInserts = 0;

dbEvents.on('enqueue', (task) => {
	dbInsertQueue.push(task);

	if (dbInsertQueue.length >= DB_BATCH_SIZE) {
		flushDbBatch();
	} else if (!dbFlushTimer) {
		dbFlushTimer = setTimeout(flushDbBatch, DB_FLUSH_INTERVAL);
	}
});

async function flushDbBatch() {
	if (dbInsertQueue.length === 0) return;

	if (dbFlushTimer) {
		clearTimeout(dbFlushTimer);
		dbFlushTimer = null;
	}

	const batch = dbInsertQueue.splice(0, DB_BATCH_SIZE);

	const values = batch
		.map((t) => {
			const ts = t.timestamp;
			return `('${t.camNo}', ${ts.getFullYear()}, ${
				ts.getMonth() + 1
			}, ${ts.getDate()}, ${ts.getHours()}, ${ts.getMinutes()}, ${ts.getSeconds()}, ${ts.getMilliseconds()}, '${
				t.imgPath
			}')`;
		})
		.join(',');

	const query = `
		INSERT INTO tb_index
		(camNo, t_year, t_mon, t_mday, t_hour, t_min, t_sec, t_mill, l_location)
		VALUES ${values};
	`;

	let conn;
	try {
		conn = await pool.getConnection();
		await conn.query(query);
		totalDbInserts += batch.length;
		log(`DB: Inserted ${batch.length} records (Total: ${totalDbInserts})`);
	} catch (err) {
		log(`DB insert error: ${err.message}`, 'ERROR');
		dbInsertQueue = batch.concat(dbInsertQueue);
		setTimeout(flushDbBatch, 2000);
	} finally {
		if (conn) conn.end();
	}
}

// Storage Queue (Async Disk Writes)
const storageQueue = [];
let isStorageProcessing = false;
let totalFilesSaved = 0;

async function processStorageQueue() {
	if (isStorageProcessing || storageQueue.length === 0) return;

	isStorageProcessing = true;

	while (storageQueue.length > 0) {
		const task = storageQueue.shift();
		const filePath = path.join(BMP_FOLDER, task.filename);

		try {
			await fs.writeFile(filePath, task.imageBuffer);
			totalFilesSaved++;

			// Queue DB insert
			dbEvents.emit('enqueue', {
				camNo: task.camNo,
				timestamp: task.timestamp,
				imgPath: path.relative(process.cwd(), filePath).replace(/\\/g, '/'),
			});
		} catch (err) {
			log(`Storage error: ${task.filename} - ${err.message}`, 'ERROR');
		}
	}

	isStorageProcessing = false;
}

setInterval(processStorageQueue, 700);

// TCP Socket Server (Camera -> Node)
const tcpServer = net.createServer((socket) => {
	log('Camera connected via TCP');

	let buffer = Buffer.alloc(0);
	let waitingForMetadata = true;
	let metadataLength = 0;
	let metadata = null;
	let frameCount = 0;
	let statsStart = Date.now();
	let statsBytes = 0;

	socket.on('data', (data) => {
		buffer = Buffer.concat([buffer, data]);
		statsBytes += data.length;

		while (true) {
			// Read metadata length
			if (waitingForMetadata) {
				if (buffer.length < 4) break;
				metadataLength = buffer.readUInt32BE(0);
				buffer = buffer.slice(4);
				waitingForMetadata = false;
			}

			// Read metadata JSON
			if (!metadata && buffer.length >= metadataLength) {
				const metadataJson = buffer.slice(0, metadataLength).toString('utf-8');
				metadata = JSON.parse(metadataJson);
				buffer = buffer.slice(metadataLength);
			}

			// Read frame bytes
			if (metadata && buffer.length >= metadata.size) {
				const imageBuffer = buffer.slice(0, metadata.size);
				buffer = buffer.slice(metadata.size);

				frameCount++;

				// Broadcast to live viewers
				broadcastFrameBinary(metadata.camNo, imageBuffer, metadata.timestamp);

				// Queue for storage
				if (storageQueue.length < STORAGE_QUEUE_MAX) {
					storageQueue.push({
						camNo: metadata.camNo,
						filename: metadata.filename,
						timestamp: new Date(metadata.timestamp),
						imageBuffer: imageBuffer,
					});
				} else {
					log(`Storage queue full! Dropping frame`, 'WARN');
				}

				// Log stats every 10 frames
				if (frameCount % 10 === 0) {
					const elapsed = (Date.now() - statsStart) / 1000;
					const fps = 10 / elapsed;
					const avgSize = statsBytes / 10 / 1024;
					log(
						`Frame ${frameCount} | FPS: ${fps.toFixed(1)} | ` +
							`Size: ${avgSize.toFixed(0)}KB | ` +
							`Storage Q: ${storageQueue.length} | ` +
							`DB Q: ${dbInsertQueue.length}`
					);
					statsStart = Date.now();
					statsBytes = 0;
				}

				// Reset for next frame
				waitingForMetadata = true;
				metadataLength = 0;
				metadata = null;
			} else {
				break;
			}
		}
	});

	socket.on('close', () => {
		log(`Camera disconnected (Total frames: ${frameCount})`);
	});

	socket.on('error', (err) => {
		log(`TCP error: ${err.message}`, 'ERROR');
	});
});

tcpServer.on('error', (err) => {
	log(`TCP server error: ${err.message}`, 'ERROR');
	if (err.code === 'EADDRINUSE') {
		log(`Port ${SOCKET_PORT} already in use!`, 'ERROR');
	}
});

function makeFilenameFromTimestamp(ts) {
	const d = new Date(Number(ts));
	const yy = String(d.getFullYear()).slice(-2);
	const MM = String(d.getMonth() + 1).padStart(2, '0');
	const dd = String(d.getDate()).padStart(2, '0');
	const hh = String(d.getHours()).padStart(2, '0');
	const mm = String(d.getMinutes()).padStart(2, '0');
	const ss = String(d.getSeconds()).padStart(2, '0');
	const ms = String(d.getMilliseconds()).padStart(3, '0');
	return `${yy}${MM}${dd}${hh}${mm}${ss}_${ms}.bmp`;
}

// ---- POST /api/frames
// Body (JSON): { camNo: "CAM0", timestamp: 1730123456789, filename?: "yyMMddhhmmss_ms.bmp", imageBase64: "<base64>" }

app.post('/api/frames', (req, res) => {
	try {
		const { camNo, timestamp, filename, imageBase64 } = req.body;

		if (!camNo || !timestamp || !imageBase64) {
			return res.status(400).json({ error: 'camNo, timestamp and imageBase64 are required' });
		}

		if (storageQueue.length >= STORAGE_QUEUE_MAX) {
			log(`Storage queue full (POST) - rejecting`, 'WARN');
			return res.status(429).json({ error: 'Storage queue full. Try again later.' });
		}

		const finalFilename =
			filename && typeof filename === 'string' ? filename : makeFilenameFromTimestamp(timestamp);

		const item = {
			camNo: String(camNo),
			filename: finalFilename,
			timestamp: new Date(Number(timestamp)),
			imageBuffer: Buffer.from(imageBase64, 'base64'),
		};

		storageQueue.push(item);

		log(`POST /api/frames: queued ${finalFilename} (Queue: ${storageQueue.length})`);
		return res.json({
			status: 'queued',
			filename: finalFilename,
			storageQueue: storageQueue.length,
		});
	} catch (err) {
		log(`POST /api/frames error: ${err.message}`, 'ERROR');
		return res.status(500).json({ error: 'Internal server error' });
	}
});

// ---- GET /api/frames
// Query options:
//  - camNo (required)
//  - timestamp (epoch ms)  OR  start (epoch ms) & end (epoch ms)
//  - OR year, month, day, hour, minute, second

app.get('/api/frames', async (req, res) => {
	let conn;
	try {
		const camNo = req.query.camNo;
		if (!camNo) return res.status(400).json({ error: 'camNo is required' });

		const ts = req.query.timestamp ? Number(req.query.timestamp) : null;
		const start = req.query.start ? Number(req.query.start) : null;
		const end = req.query.end ? Number(req.query.end) : null;

		const fieldsFromMs = (ms) => {
			const d = new Date(ms);
			return {
				year: d.getFullYear(),
				mon: d.getMonth() + 1,
				mday: d.getDate(),
				hour: d.getHours(),
				min: d.getMinutes(),
				sec: d.getSeconds(),
				mill: d.getMilliseconds(),
			};
		};

		let query = `
			SELECT camNo, t_year, t_mon, t_mday, t_hour, t_min, t_sec, t_mill, l_location
			FROM tb_index
			WHERE camNo = ?
		`;
		const params = [String(camNo)];

		if (ts) {
			const f = fieldsFromMs(ts);
			query += ` AND t_year = ? AND t_mon = ? AND t_mday = ? AND t_hour = ? AND t_min = ? AND t_sec = ? `;
			params.push(f.year, f.mon, f.mday, f.hour, f.min, f.sec);
		} else if (start && end) {
			const s = fieldsFromMs(start);
			const e = fieldsFromMs(end);

			query += `
        AND (
            (t_year > ?) OR
            (t_year = ? AND t_mon > ?) OR
            (t_year = ? AND t_mon = ? AND t_mday > ?) OR
            (t_year = ? AND t_mon = ? AND t_mday = ? AND t_hour > ?) OR
            (t_year = ? AND t_mon = ? AND t_mday = ? AND t_hour = ? AND t_min > ?) OR
            (t_year = ? AND t_mon = ? AND t_mday = ? AND t_hour = ? AND t_min = ? AND t_sec > ?) OR
            (t_year = ? AND t_mon = ? AND t_mday = ? AND t_hour = ? AND t_min = ? AND t_sec = ? AND t_mill >= ?)
        )
        AND (
            (t_year < ?) OR
            (t_year = ? AND t_mon < ?) OR
            (t_year = ? AND t_mon = ? AND t_mday < ?) OR
            (t_year = ? AND t_mon = ? AND t_mday = ? AND t_hour < ?) OR
            (t_year = ? AND t_mon = ? AND t_mday = ? AND t_hour = ? AND t_min < ?) OR
            (t_year = ? AND t_mon = ? AND t_mday = ? AND t_hour = ? AND t_min = ? AND t_sec < ?) OR
            (t_year = ? AND t_mon = ? AND t_mday = ? AND t_hour = ? AND t_min = ? AND t_sec = ? AND t_mill <= ?)
        )
    `;

			params.push(
				// Start (>=)
				s.year,
				s.year,
				s.mon,
				s.year,
				s.mon,
				s.mday,
				s.year,
				s.mon,
				s.mday,
				s.hour,
				s.year,
				s.mon,
				s.mday,
				s.hour,
				s.min,
				s.year,
				s.mon,
				s.mday,
				s.hour,
				s.min,
				s.sec,
				s.year,
				s.mon,
				s.mday,
				s.hour,
				s.min,
				s.sec,
				s.mill,

				// End (<=)
				e.year,
				e.year,
				e.mon,
				e.year,
				e.mon,
				e.mday,
				e.year,
				e.mon,
				e.mday,
				e.hour,
				e.year,
				e.mon,
				e.mday,
				e.hour,
				e.min,
				e.year,
				e.mon,
				e.mday,
				e.hour,
				e.min,
				e.sec,
				e.year,
				e.mon,
				e.mday,
				e.hour,
				e.min,
				e.sec,
				e.mill
			);
		} else {
			const qYear = req.query.year ? Number(req.query.year) : null;
			const qMon = req.query.month ? Number(req.query.month) : null;
			const qDay = req.query.day ? Number(req.query.day) : null;
			const qHour = req.query.hour ? Number(req.query.hour) : null;
			const qMin = req.query.minute ? Number(req.query.minute) : null;
			const qSec = req.query.second ? Number(req.query.second) : null;

			if (qYear) {
				query += ` AND t_year = ? `;
				params.push(qYear);
			}
			if (qMon) {
				query += ` AND t_mon = ? `;
				params.push(qMon);
			}
			if (qDay) {
				query += ` AND t_mday = ? `;
				params.push(qDay);
			}
			if (qHour) {
				query += ` AND t_hour = ? `;
				params.push(qHour);
			}
			if (qMin) {
				query += ` AND t_min = ? `;
				params.push(qMin);
			}
			if (qSec) {
				query += ` AND t_sec = ? `;
				params.push(qSec);
			}
		}

		query += ` ORDER BY t_year, t_mon, t_mday, t_hour, t_min, t_sec, t_mill LIMIT 5000;`;

		conn = await pool.getConnection();
		const rows = await conn.query(query, params);

		return res.json({ count: rows.length, frames: rows });
	} catch (err) {
		log(`GET /api/frames error: ${err.message}`, 'ERROR');
		return res.status(500).json({ error: 'Internal server error' });
	} finally {
		if (conn) conn.end();
	}
});

// ---- GET /api/frame-file
// Query: ?filename=250201104512_123.bmp  or complete path as well (bmpData/241028185056_789.bmp)

app.get('/api/frame-file', (req, res) => {
	try {
		const filename = req.query.filename || req.query.file || req.query.path;
		if (!filename) return res.status(400).json({ error: 'filename query param required' });

		const fsSync = require('fs');
		const safeName = path.basename(filename);
		const fullPath = path.join(BMP_FOLDER, safeName);

		if (!fsSync.existsSync(fullPath)) {
			return res.status(404).json({ error: 'File not found' });
		}

		res.setHeader('Content-Type', 'image/bmp');
		res.setHeader('Content-Disposition', `inline; filename="${safeName}"`);
		const stream = fsSync.createReadStream(fullPath);
		stream.on('error', (err) => {
			log(`Stream error for ${safeName}: ${err.message}`, 'ERROR');
			res.status(500).end();
		});
		stream.pipe(res);
	} catch (err) {
		log(`GET /api/frame-file error: ${err.message}`, 'ERROR');
		return res.status(500).json({ error: 'Internal server error' });
	}
});

// Start Servers
server.listen(HTTP_PORT, () => {
	log(`HTTP server: http://localhost:${HTTP_PORT} (live feed)`);
	log(`Playback UI: http://localhost:${HTTP_PORT}/playback`);
});

tcpServer.listen(SOCKET_PORT, () => {
	log(`TCP server listening on port ${SOCKET_PORT}`);
});

// Status Report
setInterval(() => {
	let playbackInfo = '';
	if (playbackSessions.size > 0) {
		playbackSessions.forEach((s, camNo) => {
			playbackInfo += ` | ${camNo}: ${s.frameCount} sent, Q: ${s.fileQueue.length}`;
		});
	}

	log(
		`Status | Clients: ${wsClientCount} | ` +
			`Storage Q: ${storageQueue.length} | ` +
			`DB Q: ${dbInsertQueue.length} | ` +
			`Files saved: ${totalFilesSaved} | ` +
			`DB inserts: ${totalDbInserts}${playbackInfo}`
	);
}, 30000);

// Graceful Shutdown
process.on('SIGINT', async () => {
	log('\nShutting down gracefully...');

	// Stop all playback sessions
	for (const camNo of playbackSessions.keys()) {
		await stopPlayback(camNo);
	}

	await flushDbBatch();
	await processStorageQueue();

	tcpServer.close();
	server.close();
	await pool.end();

	log('Shutdown complete');
	process.exit(0);
});

// PLAYBACK MODULE

// Stop Playback Session
async function stopPlayback(camNo) {
	const session = playbackSessions.get(camNo);
	if (!session) return;

	session.active = false;
	playbackSessions.delete(camNo);

	log(`[PLAYBACK] Session stopped: ${camNo} | Frames sent: ${session.frameCount}`);
}

// Start Playback Session
async function startPlaybackSession(camNo) {
	const session = playbackSessions.get(camNo);
	if (!session || session.workerRunning) return;

	session.workerRunning = true;

	let lastRowKey = null;
	let fetching = false;
	let finished = false;
	let totalRowsFetched = 0;

	// Fetch Next Batch
	const fetchNextBatch = async () => {
		if (!session.active || fetching || finished) return;
		fetching = true;

		let conn;
		const queryStart = Date.now();

		try {
			conn = await pool.getConnection();

			let query;
			let params;

			if (!lastRowKey) {
				// FIRST QUERY: Start from user-specified timestamp
				const s = session.startTime;
				query = `
					SELECT t_year, t_mon, t_mday, t_hour, t_min, t_sec, t_mill, l_location
					FROM tb_index
					WHERE camNo = ?
					  AND (
						(t_year > ?) OR
						(t_year = ? AND t_mon > ?) OR
						(t_year = ? AND t_mon = ? AND t_mday > ?) OR
						(t_year = ? AND t_mon = ? AND t_mday = ? AND t_hour > ?) OR
						(t_year = ? AND t_mon = ? AND t_mday = ? AND t_hour = ? AND t_min > ?) OR
						(t_year = ? AND t_mon = ? AND t_mday = ? AND t_hour = ? AND t_min = ? AND t_sec > ?) OR
						(t_year = ? AND t_mon = ? AND t_mday = ? AND t_hour = ? AND t_min = ? AND t_sec = ? AND t_mill >= ?)
					  )
					ORDER BY t_year, t_mon, t_mday, t_hour, t_min, t_sec, t_mill
					LIMIT ?;
				`;
				params = [
					camNo,
					s.getFullYear(),
					s.getFullYear(),
					s.getMonth() + 1,
					s.getFullYear(),
					s.getMonth() + 1,
					s.getDate(),
					s.getFullYear(),
					s.getMonth() + 1,
					s.getDate(),
					s.getHours(),
					s.getFullYear(),
					s.getMonth() + 1,
					s.getDate(),
					s.getHours(),
					s.getMinutes(),
					s.getFullYear(),
					s.getMonth() + 1,
					s.getDate(),
					s.getHours(),
					s.getMinutes(),
					s.getSeconds(),
					s.getFullYear(),
					s.getMonth() + 1,
					s.getDate(),
					s.getHours(),
					s.getMinutes(),
					s.getSeconds(),
					s.getMilliseconds(),
					PLAYBACK_BATCH_SIZE,
				];
			} else {
				// CONTINUATION QUERY: Start AFTER last row (including milliseconds)
				const lr = lastRowKey;
				query = `
					SELECT t_year, t_mon, t_mday, t_hour, t_min, t_sec, t_mill, l_location
					FROM tb_index
					WHERE camNo = ?
					  AND (
						(t_year > ?) OR
						(t_year = ? AND t_mon > ?) OR
						(t_year = ? AND t_mon = ? AND t_mday > ?) OR
						(t_year = ? AND t_mon = ? AND t_mday = ? AND t_hour > ?) OR
						(t_year = ? AND t_mon = ? AND t_mday = ? AND t_hour = ? AND t_min > ?) OR
						(t_year = ? AND t_mon = ? AND t_mday = ? AND t_hour = ? AND t_min = ? AND t_sec > ?) OR
						(t_year = ? AND t_mon = ? AND t_mday = ? AND t_hour = ? AND t_min = ? AND t_sec = ? AND t_mill > ?)
					  )
					ORDER BY t_year, t_mon, t_mday, t_hour, t_min, t_sec, t_mill
					LIMIT ?;
				`;
				params = [
					camNo,
					lr.t_year,
					lr.t_year,
					lr.t_mon,
					lr.t_year,
					lr.t_mon,
					lr.t_mday,
					lr.t_year,
					lr.t_mon,
					lr.t_mday,
					lr.t_hour,
					lr.t_year,
					lr.t_mon,
					lr.t_mday,
					lr.t_hour,
					lr.t_min,
					lr.t_year,
					lr.t_mon,
					lr.t_mday,
					lr.t_hour,
					lr.t_min,
					lr.t_sec,
					lr.t_year,
					lr.t_mon,
					lr.t_mday,
					lr.t_hour,
					lr.t_min,
					lr.t_sec,
					lr.t_mill,
					PLAYBACK_BATCH_SIZE,
				];
			}

			const rows = await conn.query(query, params);
			const queryTime = Date.now() - queryStart;

			if (rows.length === 0) {
				finished = true;

				// Check if this is the FIRST batch (no frames found at all)
				if (totalRowsFetched === 0) {
					log(`[PLAYBACK] ${camNo}: No frames found for specified time`, 'WARN');

					// Notify frontend
					if (session.ws.readyState === 1) {
						session.ws.send(
							JSON.stringify({
								type: 'playback-no-data',
								camNo,
								message: 'No frames found for the specified time period',
							})
						);
					}
				} else {
					log(`[PLAYBACK] ${camNo}: No more frames (total fetched: ${totalRowsFetched})`);
				}

				return;
			}

			totalRowsFetched += rows.length;
			log(
				`[PLAYBACK] ${camNo}: Fetched ${rows.length} rows in ${queryTime}ms (total: ${totalRowsFetched})`
			);

			lastRowKey = rows[rows.length - 1];

			// Add to file queue
			for (const r of rows) {
				session.fileQueue.push({
					filePath: path.resolve(r.l_location),
					timestamp: new Date(
						r.t_year,
						r.t_mon - 1,
						r.t_mday,
						r.t_hour,
						r.t_min,
						r.t_sec,
						r.t_mill
					).getTime(),
				});
			}
		} catch (err) {
			log(`[PLAYBACK] ${camNo}: DB error - ${err.message}`, 'ERROR');
			finished = true;
		} finally {
			if (conn) conn.end();
			fetching = false;
		}
	};

	//  File Reader Worker
	const worker = async () => {
		let readTimes = [];
		let consecutiveErrors = 0;
		const MAX_CONSECUTIVE_ERRORS = 20;

		while (session.active) {
			// Trigger fetch if queue is low and more data available
			if (session.fileQueue.length <= PLAYBACK_QUEUE_LOW && !fetching && !finished) {
				fetchNextBatch();
			}

			// Wait if queue is empty
			if (session.fileQueue.length === 0) {
				if (finished) {
					// Playback complete
					log(`[PLAYBACK] ${camNo}: Complete - ${session.frameCount} frames sent`);

					if (session.ws.readyState === 1) {
						session.ws.send(
							JSON.stringify({
								type: 'playback-complete',
								camNo,
								totalFrames: session.frameCount,
							})
						);
					}

					break;
				}

				// Wait for fetch to complete
				await new Promise((r) => setTimeout(r, 100));
				continue;
			}

			// Check for pause
			if (session.paused) {
				await new Promise((r) => setTimeout(r, 100));
				continue;
			}

			const frame = session.fileQueue.shift();

			try {
				// Read BMP file
				const readStart = Date.now();
				let imageBuffer;
				let actualPath = frame.filePath;

				try {
					imageBuffer = await fs.readFile(frame.filePath);
				} catch (err) {
					if (frame.filePath.includes('generated')) {
						const fallbackPath = frame.filePath.replace(/[/\\]generated[/\\]/, '/');
						try {
							imageBuffer = await fs.readFile(fallbackPath);
							actualPath = fallbackPath;
							log(`[PLAYBACK] ${camNo}: Found file at alternate path`, 'WARN');
						} catch (fallbackErr) {
							throw err;
						}
					} else {
						throw err;
					}
				}

				const readTime = Date.now() - readStart;

				readTimes.push(readTime);
				if (readTimes.length > 20) readTimes.shift();

				// Create binary payload
				const header = JSON.stringify({
					camNo,
					timestamp: frame.timestamp,
					type: 'playback',
				});
				const headerBuf = Buffer.from(header);
				const headerLen = Buffer.alloc(4);
				headerLen.writeUInt32BE(headerBuf.length, 0);
				const payload = Buffer.concat([headerLen, headerBuf, imageBuffer]);

				// Send frame
				if (session.ws.readyState === 1) {
					session.ws.send(payload);
					session.frameCount++;
					consecutiveErrors = 0;
				} else {
					log(`[PLAYBACK] ${camNo}: WebSocket closed, stopping`, 'WARN');
					session.active = false;
					break;
				}

				// Log progress every 10 frames
				if (session.frameCount % 10 === 0) {
					const avgRead = (readTimes.reduce((a, b) => a + b, 0) / readTimes.length).toFixed(1);
					log(
						`[PLAYBACK] ${camNo}: Frame ${session.frameCount} | ` +
							`Queue: ${session.fileQueue.length} | ` +
							`Read: ${avgRead}ms | ` +
							`Delay: ${(PLAYBACK_DELAY_MS / session.speed).toFixed(0)}ms`
					);
				}

				// Playback delay (adjusted by speed)
				const delay = PLAYBACK_DELAY_MS / session.speed;
				await new Promise((r) => setTimeout(r, delay));
			} catch (err) {
				consecutiveErrors++;
				log(
					`[PLAYBACK] ${camNo}: File error - ${frame.filePath.split(/[/\\]/).pop()} - ${
						err.message
					}`,
					'ERROR'
				);

				// If too many consecutive errors, assume all files are missing
				if (consecutiveErrors >= MAX_CONSECUTIVE_ERRORS) {
					log(
						`[PLAYBACK] ${camNo}: Too many consecutive errors (${consecutiveErrors}), stopping playback`,
						'ERROR'
					);

					if (session.ws.readyState === 1) {
						session.ws.send(
							JSON.stringify({
								type: 'playback-no-frames',
								camNo,
								message:
									'Frame files not found on disk. Database has metadata but files are missing.',
							})
						);
					}

					session.active = false;
					break;
				}

				// Notify frontend of missing frame (only every 5 errors to avoid spam)
				if (consecutiveErrors % 5 === 0 && session.ws.readyState === 1) {
					session.ws.send(
						JSON.stringify({
							type: 'frame-missing',
							timestamp: frame.timestamp,
							filename: frame.filePath.split(/[/\\]/).pop(),
							consecutiveErrors: consecutiveErrors,
							message: 'Frame files missing from disk',
						})
					);
				}

				// Continue to next frame
			}
		}

		// Cleanup
		log(`[PLAYBACK] ${camNo}: Worker stopped (${session.frameCount} frames sent)`);
		playbackSessions.delete(camNo);
		session.workerRunning = false;
	};

	// Start first batch fetch, then start worker
	await fetchNextBatch();
	worker();
}
