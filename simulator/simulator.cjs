require('dotenv').config();
const fs = require('fs');
const path = require('path');
const axios = require('axios');
const Jimp = require('jimp');
const mariadb = require('mariadb');
const NodeWebcam = require('node-webcam');

// --- CONFIG ---
const MODE = process.env.SIM_MODE || 'frame'; // 'frame' | 'usb-cam'
const ROOT = path.join(__dirname, '..');
const BASE_DIR = path.join(ROOT, 'bmpData', 'base');
const OUTPUT_DIR = path.join(ROOT, 'bmpData', 'generated');
const INTERVAL = 100;
const WEBHOOK_URL = 'http://localhost:3005/api/new-frame';
const baseImages = ['image1.bmp', 'image2.bmp'];

let frameCount = 0;

// --- MariaDB connection pool ---
const pool = mariadb.createPool({
	host: process.env.DB_HOST,
	user: process.env.DB_USER,
	password: process.env.DB_PASSWORD,
	database: process.env.DB_NAME,
	connectionLimit: 5,
});

// --- Webcam Config ---
const webcam = NodeWebcam.create({
	width: 640,
	height: 480,
	quality: 100,
	output: 'bmp',
	callbackReturn: 'location',
	verbose: false,
});

if (!fs.existsSync(OUTPUT_DIR)) fs.mkdirSync(OUTPUT_DIR, { recursive: true });

// --- Utility: Generate frame name ---
function getFrameName() {
	const now = new Date();
	const yy = String(now.getFullYear()).slice(-2);
	const MM = String(now.getMonth() + 1).padStart(2, '0');
	const dd = String(now.getDate()).padStart(2, '0');
	const hh = String(now.getHours()).padStart(2, '0');
	const mm = String(now.getMinutes()).padStart(2, '0');
	const ss = String(now.getSeconds()).padStart(2, '0');
	const ms = String(now.getMilliseconds()).padStart(6, '0');

	return `${yy}${MM}${dd}${hh}${mm}${ss}_${ms}.bmp`;
}

// --- Save frame metadata to DB ---
async function saveFrameMetadata(camNo, destFile) {
	let conn;
	try {
		conn = await pool.getConnection();

		const now = new Date();

		const result = await conn.query(
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
				destFile,
			]
		);

		console.log(`🧾 Frame metadata saved (ID: ${result.insertId})`);
	} catch (err) {
		console.error('DB error:', err.message);
	} finally {
		if (conn) await conn.end();
	}
}

// --- Notify backend via webhook ---
async function notifyBackend(camNo, destFile) {
	try {
		await axios.post(WEBHOOK_URL, {
			camNo,
			l_location: path.relative(process.cwd(), destFile).replace(/\\/g, '/'),
		});
		console.log('📡 Webhook sent successfully');
	} catch (axiosErr) {
		console.error('Webhook error:', axiosErr.message);
	}
}

// --- Frame Simulator Mode (existing logic) ---
async function generateSimulatedFrame() {
	const currentFrame = ++frameCount;
	const camNo = 'CAM0';
	const srcFile = path.join(BASE_DIR, baseImages[(currentFrame - 1) % baseImages.length]);
	const destFile = path.join(OUTPUT_DIR, getFrameName());

	try {
		const image = await Jimp.read(srcFile);
		const brightness = (Math.random() - 0.5) * 0.8;
		image.brightness(brightness);

		const font = await Jimp.loadFont(Jimp.FONT_SANS_64_WHITE);
		image.print(font, 15, 15, `Frame: ${currentFrame}`);

		await image.writeAsync(destFile);
		await saveFrameMetadata(camNo, destFile);
		await notifyBackend(camNo, destFile);

		console.log(
			`🖼️ Frame #${currentFrame}: ${path.basename(destFile)} (brightness ${brightness.toFixed(2)})`
		);
	} catch (err) {
		console.error('Frame generation error:', err.message);
	}
}

// --- USB Camera Mode (new) ---
async function captureFromUSBCam() {
	const currentFrame = ++frameCount;
	const camNo = 'CAM0';
	const destFile = path.join(OUTPUT_DIR, getFrameName());

	return new Promise((resolve) => {
		NodeWebcam.capture(destFile.replace(/\.bmp$/, ''), webcam.options, async (err) => {
			if (err) {
				console.error('Camera capture error:', err.message);
				return resolve();
			}

			try {
				await saveFrameMetadata(camNo, destFile);
				await notifyBackend(camNo, destFile);
				console.log(`📸 Captured USB frame #${currentFrame}: ${path.basename(destFile)}`);
			} catch (error) {
				console.error('Error handling captured frame:', error.message);
			}

			resolve();
		});
	});
}

// --- Main Loop ---
async function startSimulator() {
	console.log(`🟢 Simulator started in "${MODE}" mode — capturing every ${INTERVAL}ms`);

	while (true) {
		if (MODE === 'usb-cam') {
			await captureFromUSBCam();
		} else {
			await generateSimulatedFrame();
		}
		await new Promise((res) => setTimeout(res, INTERVAL));
	}
}

startSimulator();

// require('dotenv').config();
// const fs = require('fs');
// const path = require('path');
// const axios = require('axios');
// const Jimp = require('jimp');
// const mariadb = require('mariadb');

// // --- CONFIG ---
// const ROOT = path.join(__dirname, '..');
// const BASE_DIR = path.join(ROOT, 'bmpData', 'base');
// const OUTPUT_DIR = path.join(ROOT, 'bmpData', 'generated');
// const INTERVAL = 200;
// const WEBHOOK_URL = 'http://localhost:3005/api/new-frame';
// const baseImages = ['image1.bmp', 'image2.bmp'];

// let frameCount = 0;

// // --- MariaDB connection pool ---
// const pool = mariadb.createPool({
// 	host: process.env.DB_HOST,
// 	user: process.env.DB_USER,
// 	password: process.env.DB_PASSWORD,
// 	database: process.env.DB_NAME,
// 	connectionLimit: 5,
// });

// if (!fs.existsSync(OUTPUT_DIR)) fs.mkdirSync(OUTPUT_DIR, { recursive: true });

// function getFrameName() {
// 	const now = new Date();
// 	const yy = String(now.getFullYear()).slice(-2);
// 	const MM = String(now.getMonth() + 1).padStart(2, '0');
// 	const dd = String(now.getDate()).padStart(2, '0');
// 	const hh = String(now.getHours()).padStart(2, '0');
// 	const mm = String(now.getMinutes()).padStart(2, '0');
// 	const ss = String(now.getSeconds()).padStart(2, '0');
// 	const ms = String(now.getMilliseconds()).padStart(6, '0');

// 	return `${yy}${MM}${dd}${hh}${mm}${ss}_${ms}.bmp`;
// }

// async function saveFrameMetadata(camNo, destFile) {
// 	let conn;
// 	try {
// 		conn = await pool.getConnection();

// 		const now = new Date();

// 		const result = await conn.query(
// 			`INSERT INTO tb_index
//         (camNo, t_year, t_mon, t_mday, t_hour, t_min, t_sec, t_mill, l_location)
//        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
// 			[
// 				camNo,
// 				now.getFullYear(),
// 				now.getMonth() + 1,
// 				now.getDate(),
// 				now.getHours(),
// 				now.getMinutes(),
// 				now.getSeconds(),
// 				now.getMilliseconds(),
// 				destFile,
// 			]
// 		);

// 		console.log(`Frame metadata saved to DB (ID: ${result.insertId})`);
// 	} catch (err) {
// 		console.error('DB error:', err.message);
// 	} finally {
// 		if (conn) await conn.end();
// 	}
// }

// async function generateFrame() {
// 	const currentFrame = ++frameCount;
// 	const camNo = 'CAM0';
// 	const srcFile = path.join(BASE_DIR, baseImages[(currentFrame - 1) % baseImages.length]);
// 	const destFile = path.join(OUTPUT_DIR, getFrameName());

// 	try {
// 		const image = await Jimp.read(srcFile);
// 		const brightness = (Math.random() - 0.5) * 0.8;
// 		image.brightness(brightness);

// 		const font = await Jimp.loadFont(Jimp.FONT_SANS_64_WHITE);
// 		image.print(font, 15, 15, `Frame: ${currentFrame}`);

// 		await image.writeAsync(destFile);

// 		await saveFrameMetadata(camNo, destFile);

// 		// Notify backend
// 		try {
// 			await axios.post(WEBHOOK_URL, {
// 				camNo,
// 				l_location: path.relative(process.cwd(), destFile).replace(/\\/g, '/'),
// 			});
// 			console.log('Webhook sent successfully');
// 		} catch (axiosErr) {
// 			console.error('Webhook error:', axiosErr.message);
// 		}

// 		console.log(
// 			`Frame #${currentFrame}: ${path.basename(destFile)} (brightness ${brightness.toFixed(2)})`
// 		);
// 	} catch (err) {
// 		console.error('Frame generation error:', err.message);
// 	}
// }

// // --- Sequential frame generation loop ---
// async function startSimulator() {
// 	console.log(`Simulator started — generating frames every ${INTERVAL}ms`);
// 	while (true) {
// 		await generateFrame();
// 		await new Promise((res) => setTimeout(res, INTERVAL));
// 	}
// }

// startSimulator();
////////////////////////////////////////////////////////////
// const fs = require('fs');
// const path = require('path');
// const axios = require('axios');
// const Jimp = require('jimp');

// const ROOT = path.join(__dirname, '..');
// const BASE_DIR = path.join(ROOT, 'bmpData', 'base');
// const OUTPUT_DIR = path.join(ROOT, 'bmpData', 'generated');

// // frame generation interval (ms)
// const INTERVAL = 500; // change to 100 for 0.1 s, or 1000 for 1 s
// const WEBHOOK_URL = 'http://localhost:3005/api/new-frame';

// const baseImages = ['image1.bmp', 'image2.bmp'];
// let frameCount = 0;

// if (!fs.existsSync(OUTPUT_DIR)) fs.mkdirSync(OUTPUT_DIR, { recursive: true });

// function getFrameName() {
// 	const now = Date.now();
// 	return `frame_${now}.bmp`;
// }

// async function generateFrame() {
// 	const srcFile = path.join(BASE_DIR, baseImages[frameCount % baseImages.length]);
// 	const destFile = path.join(OUTPUT_DIR, getFrameName());

// 	try {
// 		const image = await Jimp.read(srcFile);

// 		// Apply small random brightness change (±10 %)
// 		const brightness = (Math.random() - 0.5) * 0.8; // much stronger range
// 		image.brightness(brightness);

// 		// Add frame number as overlay text
// 		const font = await Jimp.loadFont(Jimp.FONT_SANS_64_WHITE);
// 		image.print(font, 15, 15, `Frame: ${frameCount + 1}`);

// 		await image.writeAsync(destFile);

// 		// Notify backend
// 		await axios.post(WEBHOOK_URL, {
// 			camNo: 'CAM0',
// 			l_location: path.relative(process.cwd(), destFile).replace(/\\/g, '/'),
// 		});

// 		console.log(
// 			`📸 New frame #${frameCount + 1}: ${path.basename(destFile)} (brightness ${brightness.toFixed(
// 				2
// 			)})`
// 		);
// 	} catch (err) {
// 		console.error('❌ Error generating frame:', err.message);
// 	}

// 	frameCount++;
// }

// console.log('Simulator started — generating frames every', INTERVAL, 'ms');
// setInterval(generateFrame, INTERVAL);
