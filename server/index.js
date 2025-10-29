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

// Logging utility
function log(message, level = 'INFO') {
	const timestamp = new Date().toTimeString().substring(0, 12);
	console.log(`[${timestamp}] [${level}] ${message}`);
}

log('Node.js server starting...');

// Create storage folder
fs.mkdir(BMP_FOLDER, { recursive: true })
	.then(() => log(`Storage folder: ${BMP_FOLDER}`))
	.catch((err) => log(`Storage folder error: ${err.message}`, 'ERROR'));

// --- Express + WebSocket Setup ---
const app = express();
app.use(express.static(path.resolve(__dirname, '../frontend')));
app.get('/', (req, res) => {
	res.sendFile(path.join(__dirname, '../index.html'));
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

log(`MariaDB pool created (${DB_HOST}/${DB_NAME}/${DB_USER}/${DB_PORT}/...)`);

// Test DB connection
pool
	.getConnection()
	.then((conn) => {
		log('Database connection: OK');
		conn.end();
	})
	.catch((err) => log(`Database connection failed: ${err.message}`, 'ERROR'));

// WebSocket tracking
let wsClientCount = 0;
wss.on('connection', (ws) => {
	wsClientCount++;
	log(`Frontend connected (Total: ${wsClientCount})`);
	ws.on('close', () => {
		wsClientCount--;
		log(`Frontend disconnected (Remaining: ${wsClientCount})`);
	});
});

// --- Broadcast Frame to Frontend ---
function broadcastFrameBinary(camNo, imageBuffer, timestamp) {
	if (wss.clients.size === 0) return;

	const header = JSON.stringify({ camNo, timestamp });
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

// --- Event-Driven DB Insert Queue ---
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

// --- Storage Queue (Async Disk Writes) ---
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

// Process storage queue every 700ms
setInterval(processStorageQueue, 700);

// --- TCP Socket Server (Python -> Node) ---
const tcpServer = net.createServer((socket) => {
	log('Python camera connected');

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

				// Broadcast immediately
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
		log(`Python disconnected (Total frames: ${frameCount})`);
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

// --- Start Servers ---
server.listen(HTTP_PORT, () => {
	log(`HTTP server running on http://localhost:${HTTP_PORT}`);
});

tcpServer.listen(SOCKET_PORT, () => {
	log(`TCP server listening on port ${SOCKET_PORT}`);
});

// Status report every 30 seconds
setInterval(() => {
	log(
		`Status | Clients: ${wsClientCount} | ` +
			`Storage Q: ${storageQueue.length} | ` +
			`DB Q: ${dbInsertQueue.length} | ` +
			`Files saved: ${totalFilesSaved} | ` +
			`DB inserts: ${totalDbInserts}`
	);
}, 30000);

// Graceful shutdown
process.on('SIGINT', async () => {
	log('\nShutting down...');
	await flushDbBatch();
	await processStorageQueue();
	tcpServer.close();
	server.close();
	await pool.end();
	log('Shutdown complete');
	process.exit(0);
});
