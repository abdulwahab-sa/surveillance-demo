import cv2
import os
import socket
import json
import time
from datetime import datetime

# --- CONFIG ---
BMP_DIR = os.path.join(os.path.dirname(__file__), "bmpData")

CAMERA_ID = 0  # USB camera ID
CAPTURE_INTERVAL = 0.2 
NODE_HOST = "localhost"
NODE_PORT = 9000

os.makedirs(BMP_DIR, exist_ok=True)

def get_frame_name():
    now = datetime.now()
    return now.strftime("%y%m%d%H%M%S_%f") + ".bmp"

def send_metadata(sock, camNo, file_path):
    data = {
        "camNo": camNo,
        "file": file_path,
        "timestamp": int(time.time() * 1000)
    }
    sock.sendall((json.dumps(data) + "\n").encode("utf-8"))

def main():
    cam = cv2.VideoCapture(CAMERA_ID)
    if not cam.isOpened():
        print(" Could not open camera.")
        return

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((NODE_HOST, NODE_PORT))
    print(" Connected to Node.js server")

    try:
        while True:
            ret, frame = cam.read()
            if not ret:
                print(" Frame capture failed.")
                continue

            file_name = get_frame_name()
            file_path = os.path.join(BMP_DIR, file_name)

            # Save BMP frame
            cv2.imwrite(file_path, frame, [int(cv2.IMWRITE_JPEG_QUALITY), 95])
            print(f" Saved {file_name}")

            # Send metadata
            rel_path = os.path.relpath(file_path, os.getcwd()).replace("\\", "/")
            send_metadata(sock, "CAM0", rel_path)
            print(f" Sent metadata for {file_name}")

            time.sleep(CAPTURE_INTERVAL)

    except KeyboardInterrupt:
        print("\n Capture stopped by user.")
    finally:
        cam.release()
        sock.close()

if __name__ == "__main__":
    main()
