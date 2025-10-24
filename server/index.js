require('dotenv').config();
const express = require('express');
const http = require('http');
const { WebSocketServer } = require('ws');
const path = require('path');
const fs = require('fs');
const EventEmitter = require('events');
const mariadb = require('mariadb');

// --- CONFIG ---
const DEMO_MODE = process.env.DEMO_MODE || 'filesystem'; // 'filesystem' | 'database'
const PORT = process.env.PORT || 3005;
const BMP_FOLDER = path.resolve(process.env.BMP_FOLDER || './bmpData/generated');

// --- EVENT BUS ---
const eventBus = new EventEmitter();

// --- EXPRESS + WEBSOCKET SETUP ---
const app = express();
app.use(express.json());
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

wss.on('connection', () => {
	console.log('WebSocket client connected');
});

// --- BROADCAST HELPER ---
function broadcastFrame(data) {
	const payload = JSON.stringify(data);
	wss.clients.forEach((client) => {
		if (client.readyState === 1) client.send(payload);
	});
}

// --- MariaDB connection ---
const pool = mariadb.createPool({
	host: process.env.DB_HOST,
	user: process.env.DB_USER,
	password: process.env.DB_PASSWORD,
	database: process.env.DB_NAME,
	connectionLimit: 5,
});

// --- New binary broadcast version ---
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
		if (client.readyState === 1) {
			client.send(payload);
		}
	});
}

// --- EVENT HANDLER ---
eventBus.on('newFrame', async (frameInfo) => {
	try {
		let imgPath;

		if (DEMO_MODE === 'filesystem') {
			imgPath = path.resolve(frameInfo.l_location);
		} else if (DEMO_MODE === 'database') {
			let conn;
			try {
				conn = await pool.getConnection();
				const rows = await conn.query(
					'SELECT l_location FROM tb_index WHERE camNo = ? ORDER BY id DESC LIMIT 1',
					[frameInfo.camNo]
				);
				if (rows.length > 0) {
					imgPath = path.resolve(rows[0].l_location);
					console.log(`DB lookup path for ${frameInfo.camNo}: ${imgPath}`);
				} else {
					console.warn('No record found in DB for', frameInfo.camNo);
					return;
				}
			} catch (err) {
				console.error('DB query error:', err.message);
				return;
			} finally {
				if (conn) conn.end();
			}
		}

		if (imgPath && fs.existsSync(imgPath)) {
			const imageBuffer = fs.readFileSync(imgPath);
			broadcastFrameBinary(frameInfo.camNo, imageBuffer);

			console.log(`Broadcasted frame: ${path.basename(imgPath)}`);
		} else {
			console.warn('File not found:', imgPath);
		}
	} catch (err) {
		console.error('Error handling new frame:', err.message);
	}
});

// --- API ENDPOINT (Simulator triggers this) ---
app.post('/api/new-frame', (req, res) => {
	const { camNo, l_location } = req.body;
	if (!camNo) return res.status(400).json({ error: 'Missing camNo' });

	// simulator sends both, but DB mode may ignore l_location
	eventBus.emit('newFrame', { camNo, l_location });
	res.json({ status: 'received' });
});

// --- SERVE FRONTEND ---
app.use(express.static(path.resolve(__dirname, '../frontend')));

// --- START SERVER ---
server.listen(PORT, () => {
	console.log(`Backend running on http://localhost:${PORT}`);
	console.log(`DEMO_MODE = ${DEMO_MODE}`);
});

// require('dotenv').config();
// const express = require('express');
// const http = require('http');
// const { WebSocketServer } = require('ws');
// const path = require('path');
// const fs = require('fs');
// const EventEmitter = require('events');

// // --- CONFIG ---
// const DEMO_MODE = process.env.DEMO_MODE || 'filesystem';
// const PORT = process.env.PORT || 3000;
// const BMP_FOLDER = path.resolve(process.env.BMP_FOLDER || './bmpData/generated');

// // --- EVENT BUS ---
// const eventBus = new EventEmitter();

// // --- EXPRESS + WEBSOCKET SETUP ---
// const app = express();
// app.use(express.json());
// const server = http.createServer(app);
// const wss = new WebSocketServer({ server });

// wss.on('connection', () => {
// 	console.log('🟢 WebSocket client connected');
// });

// // --- BROADCAST HELPER ---
// function broadcastFrame(data) {
// 	const payload = JSON.stringify(data);
// 	wss.clients.forEach((client) => {
// 		if (client.readyState === 1) client.send(payload);
// 	});
// }

// // --- EVENT HANDLER ---
// eventBus.on('newFrame', async (frameInfo) => {
// 	try {
// 		const imgPath = path.resolve(frameInfo.l_location);
// 		if (fs.existsSync(imgPath)) {
// 			const imageBuffer = fs.readFileSync(imgPath);
// 			const base64 = imageBuffer.toString('base64');

// 			broadcastFrame({
// 				camNo: frameInfo.camNo,
// 				timestamp: Date.now(),
// 				data: base64,
// 			});

// 			console.log(`📤 Broadcasted frame: ${path.basename(imgPath)}`);
// 		} else {
// 			console.warn('⚠️ File not found:', imgPath);
// 		}
// 	} catch (err) {
// 		console.error('❌ Error handling new frame:', err.message);
// 	}
// });

// // --- API ENDPOINT (Simulator triggers this) ---
// app.post('/api/new-frame', (req, res) => {
// 	const { camNo, l_location } = req.body;
// 	if (!l_location) return res.status(400).json({ error: 'Missing l_location' });

// 	eventBus.emit('newFrame', { camNo, l_location });
// 	res.json({ status: 'received' });
// });

// // --- SERVE FRONTEND ---
// app.use(express.static(path.resolve(__dirname, '../frontend')));

// // --- START SERVER ---
// server.listen(PORT, () => {
// 	console.log(`🚀 Backend running on http://localhost:${PORT}`);
// 	console.log(`📁 Watching folder: ${BMP_FOLDER}`);
// });

// const express = require('express');
// const http = require('http');
// const { WebSocketServer } = require('ws');
// const path = require('path');
// const fs = require('fs');

// const app = express();
// app.use(express.json());

// // --- folders ---
// const BMP_FOLDER = path.resolve(__dirname, '../bmpData/generated');

// // --- 1. HTTP + WebSocket setup ---
// const server = http.createServer(app);
// const wss = new WebSocketServer({ server });

// // keep list of active WebSocket connections
// wss.on('connection', (ws) => {
// 	console.log('🟢 WebSocket connected');
// });

// // --- 2. Webhook endpoint (/api/new-frame) ---
// app.post('/api/new-frame', async (req, res) => {
// 	const { camNo, l_location } = req.body;
// 	if (!l_location) return res.status(400).json({ error: 'Missing l_location' });

// 	const imgPath = path.resolve(l_location);

// 	if (!fs.existsSync(imgPath)) {
// 		console.error('File not found:', imgPath);
// 		return res.status(404).json({ error: 'Image not found' });
// 	}

// 	// read the image as base64
// 	const imageBuffer = fs.readFileSync(imgPath);
// 	const base64 = imageBuffer.toString('base64');

// 	// broadcast to all connected WebSocket clients
// 	const payload = JSON.stringify({
// 		camNo,
// 		timestamp: Date.now(),
// 		data: base64,
// 	});

// 	wss.clients.forEach((client) => {
// 		if (client.readyState === 1) {
// 			client.send(payload);
// 		}
// 	});

// 	console.log('📤 Broadcasted frame:', path.basename(imgPath));
// 	return res.json({ status: 'ok' });
// });

// // --- 3. Serve frontend files (we’ll add next) ---
// app.use(express.static(path.resolve(__dirname, '../frontend')));

// const PORT = 3005;
// server.listen(PORT, () => console.log(`🚀 Server running on http://localhost:${PORT}`));
