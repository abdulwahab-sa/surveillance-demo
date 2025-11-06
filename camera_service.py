import cv2
import socket
import json
import time
import struct
from datetime import datetime

# --- CONFIG ---
CAMERA_ID = 0
CAPTURE_INTERVAL = 0.3  
NODE_HOST = "localhost"
NODE_PORT = 9000

def log(message, level="INFO"):
    timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    print(f"[{timestamp}] [{level}] {message}")

def get_timestamp():
    return int(time.time() * 1000)

def get_frame_name():
    now = datetime.now()
    return now.strftime("%y%m%d%H%M%S_%f") + ".bmp"

def send_frame_data(sock, cam_no, frame_bytes, timestamp, filename):
    try:
        metadata = {
            "camNo": cam_no,
            "timestamp": timestamp,
            "filename": filename,
            "size": len(frame_bytes)
        }
        metadata_json = json.dumps(metadata).encode('utf-8')
        metadata_length = struct.pack('!I', len(metadata_json))

        sock.sendall(metadata_length)
        sock.sendall(metadata_json)
        sock.sendall(frame_bytes)
        return True

    except (BrokenPipeError, ConnectionResetError):
        return False
    except Exception as e:
        log(f"Send error: {e}", "ERROR")
        return False

def main():
    log("Camera capture service starting...")

    cam = cv2.VideoCapture(CAMERA_ID)
    if not cam.isOpened():
        log("FATAL: Could not open camera!", "ERROR")
        log("Check: ls -l /dev/video* to verify camera is connected", "ERROR")
        return

    cam.set(cv2.CAP_PROP_BUFFERSIZE, 1)
    width = int(cam.get(cv2.CAP_PROP_FRAME_WIDTH))
    height = int(cam.get(cv2.CAP_PROP_FRAME_HEIGHT))
    log(f"Camera opened: {width}x{height}")

    # Connect to Node.js
    log(f"Connecting to Node.js at {NODE_HOST}:{NODE_PORT}...")
    sock = None
    while sock is None:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((NODE_HOST, NODE_PORT))
            log("Connected to Node.js server")
            break
        except ConnectionRefusedError:
            log("Waiting for Node.js server...", "WARN")
            time.sleep(2)

    log(f"Starting capture at {1/CAPTURE_INTERVAL:.1f} FPS")

    frame_count = 0
    stats_capture = stats_encode = stats_send = 0
    stats_start = time.time()

    try:
        while True:
            loop_start = time.time()

            # Capture frame
            t1 = time.time()
            ret, frame = cam.read()
            if not ret:
                log("Frame capture failed", "WARN")
                continue
            capture_time = (time.time() - t1) * 1000

            # Encode to BMP (no compression)
            t2 = time.time()
            ret, bmp_buffer = cv2.imencode('.bmp', frame)
            if not ret:
                log("Frame encoding failed", "WARN")
                continue
            encode_time = (time.time() - t2) * 1000

            frame_bytes = bmp_buffer.tobytes()
            timestamp = get_timestamp()
            filename = get_frame_name()

            # Send to Node.js
            t3 = time.time()
            if not send_frame_data(sock, "CAM0", frame_bytes, timestamp, filename):
                log("Connection lost, reconnecting...", "WARN")
                sock.close()
                while True:
                    try:
                        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        sock.connect((NODE_HOST, NODE_PORT))
                        log("Reconnected to Node.js")
                        break
                    except:
                        time.sleep(1)
                continue

            send_time = (time.time() - t3) * 1000

            frame_count += 1
            stats_capture += capture_time
            stats_encode += encode_time
            stats_send += send_time

            # Log every 10 frames
            if frame_count % 10 == 0:
                elapsed = time.time() - stats_start
                fps = 10 / elapsed
                log(f"Frame {frame_count} | FPS: {fps:.1f} | "
                    f"Capture: {stats_capture/10:.1f}ms | "
                    f"Encode: {stats_encode/10:.1f}ms | "
                    f"Send: {stats_send/10:.1f}ms | "
                    f"Size: {len(frame_bytes)/1024:.0f}KB")

                stats_capture = stats_encode = stats_send = 0
                stats_start = time.time()

            loop_time = (time.time() - loop_start) * 1000
            if loop_time > CAPTURE_INTERVAL * 1000:
                log(f"WARNING: Loop took {loop_time:.0f}ms (target: {CAPTURE_INTERVAL*1000:.0f}ms)", "WARN")

            time.sleep(max(0, CAPTURE_INTERVAL - (time.time() - loop_start)))

    except KeyboardInterrupt:
        log("\nStopped by user (Ctrl+C)")
    except Exception as e:
        log(f"FATAL ERROR: {e}", "ERROR")
        import traceback
        traceback.print_exc()
    finally:
        cam.release()
        sock.close()
        log(f"Shutdown complete. Total frames: {frame_count}")

if __name__ == "__main__":
    main()



