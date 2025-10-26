require('dotenv').config();
const net = require('net');
const { WebSocketServer } = require('ws');
const mariadb = require('mariadb');
const path = require('path');
const express = require('express');
const http = require('http');
const fs = require('fs');
const EventEmitter = require('events');

// --- CONFIG ---
const SOCKET_PORT = 9000;
const HTTP_PORT = 3005;
const BMP_FOLDER = path.resolve(process.env.BMP_FOLDER || '../bmpData');
const BATCH_SIZE = 10;
const FLUSH_INTERVAL = 500;

const app = express();
app.use(express.static(path.resolve(__dirname, '../frontend')));
app.get('/', (req, res) => {
	res.sendFile(path.join(__dirname, '../index.html'));
});

const server = http.createServer(app);
const wss = new WebSocketServer({ server });

// --- MariaDB connection pool ---
const pool = mariadb.createPool({
	host: process.env.DB_HOST,
	user: process.env.DB_USER,
	password: process.env.DB_PASSWORD,
	database: process.env.DB_NAME,
	connectionLimit: 5,
});

wss.on('connection', () => {
	console.log('Frontend connected via WebSocket');
});

// --- Broadcast Frame to Frontend ---
function broadcastFrameBinary(camNo, imageBuffer) {
	const header = JSON.stringify({
		camNo,
		timestamp: Date.now(),
	});
	const headerBuffer = Buffer.from(header);
	const headerLength = Buffer.alloc(4);
	headerLength.writeUInt32BE(headerBuffer.length, 0);
	const payload = Buffer.concat([headerLength, headerBuffer, imageBuffer]);

	wss.clients.forEach((client) => {
		if (client.readyState === 1) client.send(payload);
	});
}

// --- Event-driven DB Insert Queue ---
const dbEvents = new EventEmitter();
let insertQueue = [];
let flushTimer = null;

dbEvents.on('enqueue', (task) => {
	insertQueue.push(task);

	if (insertQueue.length >= BATCH_SIZE) {
		flushBatch();
	} else {
		if (!flushTimer) {
			flushTimer = setTimeout(flushBatch, FLUSH_INTERVAL);
		}
	}
});

async function flushBatch() {
	if (insertQueue.length === 0) return;
	if (flushTimer) {
		clearTimeout(flushTimer);
		flushTimer = null;
	}

	const batch = insertQueue.splice(0, BATCH_SIZE);
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
		console.log(`Inserted ${batch.length} records (batch)`);
	} catch (err) {
		console.error('DB batch insert error:', err.message);
		// Push back the failed batch for retry
		insertQueue = batch.concat(insertQueue);
		setTimeout(() => flushBatch(), 1000);
	} finally {
		if (conn) conn.end();
	}
}

// --- TCP Socket Server (Python -> Node) ---
const tcpServer = net.createServer((socket) => {
	console.log('Python capture connected');

	socket.on('data', async (data) => {
		try {
			const msg = JSON.parse(data.toString().trim());
			const { camNo, file, timestamp } = msg;

			const imgPath = path.isAbsolute(file) ? file : path.resolve(BMP_FOLDER, path.basename(file));

			dbEvents.emit('enqueue', {
				camNo,
				timestamp: new Date(timestamp),
				imgPath,
			});

			fs.readFile(imgPath, (err, imageBuffer) => {
				if (!err && imageBuffer) {
					broadcastFrameBinary(camNo, imageBuffer);
					console.log(`Broadcasted frame: ${path.basename(imgPath)}`);
				} else {
					console.warn(`File not found or unreadable: ${imgPath}`);
				}
			});
		} catch (err) {
			console.error('Error parsing TCP data:', err.message);
		}
	});

	socket.on('close', () => console.log('Python connection closed'));
	socket.on('error', (err) => console.error('Socket error:', err.message));
});

// --- Start Servers ---
server.listen(HTTP_PORT, () => {
	console.log(`HTTP + WebSocket server running on http://localhost:${HTTP_PORT}`);
});

tcpServer.listen(SOCKET_PORT, () => {
	console.log(`TCP socket server listening on port ${SOCKET_PORT}`);
});
