require('dotenv').config();
const net = require('net');
const { WebSocketServer } = require('ws');
const mariadb = require('mariadb');
const path = require('path');
const express = require('express');
const http = require('http');
const fs = require('fs');

// --- CONFIG ---
const SOCKET_PORT = 9000;
const HTTP_PORT = 3005;
const BMP_FOLDER = path.resolve(process.env.BMP_FOLDER || '../bmpData');

const app = express();

app.use(express.static(path.resolve(__dirname, '../frontend')));
app.get('/', (req, res) => {
	res.sendFile(path.join(__dirname, '../index.html'));
});

const server = http.createServer(app);
const wss = new WebSocketServer({ server });

// --- MariaDB connection ---
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

// --- TCP Socket Server (Python -> Node) ---
const tcpServer = net.createServer((socket) => {
	console.log('Python capture connected');

	socket.on('data', async (data) => {
		try {
			const msg = JSON.parse(data.toString().trim());
			const { camNo, file, timestamp } = msg;

			const imgPath = path.isAbsolute(file) ? file : path.resolve(BMP_FOLDER, path.basename(file));

			// --- Save metadata in MariaDB ---
			const now = new Date(timestamp);
			let conn;
			try {
				conn = await pool.getConnection();
				await conn.query(
					`INSERT INTO tb_index 
					(camNo, t_year, t_mon, t_mday, t_hour, t_min, t_sec, t_mill, l_location)
					VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
					[
						camNo,
						now.getFullYear(),
						now.getMonth() + 1,
						now.getDate(),
						now.getHours(),
						now.getMinutes(),
						now.getSeconds(),
						now.getMilliseconds(),
						imgPath,
					]
				);
				console.log(`Metadata inserted for ${path.basename(file)}`);
			} catch (err) {
				console.error('DB error:', err.message);
			} finally {
				if (conn) conn.end();
			}

			// --- Read and broadcast BMP frame ---
			if (fs.existsSync(imgPath)) {
				const imageBuffer = fs.readFileSync(imgPath);
				broadcastFrameBinary(camNo, imageBuffer);
				console.log(`Broadcasted frame: ${path.basename(imgPath)}`);
			} else {
				console.warn(`File not found: ${imgPath}`);
			}
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
