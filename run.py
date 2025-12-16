# run.py
import os
import sys
import webbrowser
import threading
import time
import subprocess
from datetime import datetime

# === Add project root to path ===
project_dir = os.path.dirname(os.path.abspath(__file__))
if project_dir not in sys.path:
    sys.path.insert(0, project_dir)

# === Import Flask app and updater from server ===
try:
    from server.app import app, updater
    print("[OK] Imported app and updater from server/app.py")
except ImportError as e:
    print(f"[FATAL] Cannot import from server/app.py: {e}")
    print("Make sure server/app.py exists and has 'app' and 'updater' defined.")
    sys.exit(1)

# === CONFIG ===
APP_VERSION = "1.0.0"
DASHBOARD_URL = "http://127.0.0.1:5000"
HOST = "127.0.0.1"
PORT = 5000

# === Optional: Auto-update (disabled by default) ===
ENABLE_AUTO_UPDATE = False
UPDATE_URL = "https://yourdomain.com/updates/TSE_Options_Analyzer.exe"
VERSION_URL = "https://yourdomain.com/updates/version.txt"

# === Helpers ===
def print_banner():
    print("=" * 70)
    print(" TSE Options Chain Analyzer - Mini Launcher")
    print(f" Version: {APP_VERSION} | {datetime.now().strftime('%Y/%m/%d %H:%M:%S')}")
    print(f" Dashboard: {DASHBOARD_URL}")
    print("=" * 70)

def open_browser():
    """Open dashboard in default browser after server starts"""
    time.sleep(2.5)
    try:
        webbrowser.open(DASHBOARD_URL)
        print(f"[BROWSER] Opened: {DASHBOARD_URL}")
    except Exception as e:
        print(f"[BROWSER] Failed to open browser: {e}")

def check_server_ready():
    """Wait until server responds on /api/status"""
    import requests
    url = f"{DASHBOARD_URL}/api/status"
    for i in range(20):
        try:
            r = requests.get(url, timeout=3)
            if r.status_code == 200:
                print("[SERVER] Ready and responding")
                return True
        except:
            print(f"[SERVER] Waiting... ({i+1}/20)")
            time.sleep(1)
    print("[SERVER] Timeout: Server not responding")
    return False

def auto_update():
    """Optional: Check for new version (for .exe builds)"""
    if not ENABLE_AUTO_UPDATE:
        print("[UPDATE] Auto-update is disabled")
        return
    try:
        import requests
        print("[UPDATE] Checking for updates...")
        r = requests.get(VERSION_URL, timeout=10)
        latest = r.text.strip()
        if latest != APP_VERSION:
            print(f"[UPDATE] New version available: {latest} (current: {APP_VERSION})")
            print(f"Download: {UPDATE_URL}")
        else:
            print(f"[UPDATE] Already up to date: {APP_VERSION}")
    except Exception as e:
        print(f"[UPDATE] Check failed: {e}")

# === Main ===
def main():
    print_banner()

    # 1. Optional update check
    auto_update()

    # 2. Start background data updater
    print("[1/3] Starting background data updater (every 5 minutes)...")
    updater.start()
    print("[OK] Data updater started")

    # 3. Start Flask server in background thread
    print(f"[2/3] Starting web server on {DASHBOARD_URL}...")
    server_thread = threading.Thread(
        target=app.run,
        kwargs={
            'host': HOST,
            'port': PORT,
            'debug': False,
            'use_reloader': False,
            'threaded': True
        },
        daemon=True
    )
    server_thread.start()

    # 4. Wait for server + open browser
    print("[3/3] Waiting for server to start...")
    if check_server_ready():
        threading.Thread(target=open_browser, daemon=True).start()
    else:
        print("[WARN] Server may not have started correctly. Check logs.")

    # Keep main thread alive
    print("[READY] System is running. Press Ctrl+C to stop.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n[SHUTDOWN] Stopping server...")
    finally:
        updater.stop()
        print("[OK] Data updater stopped")
        print("[EXIT] Goodbye!")

# === Run ===
if __name__ == '__main__':
    main()