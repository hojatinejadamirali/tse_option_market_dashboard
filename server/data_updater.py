# server/data_updater.py
import threading
import time
import importlib.util
import os
import traceback
from datetime import datetime
from typing import List, Tuple, Dict, Any


class DataUpdater:
    """
    Background updater for TSE Options Chain pipeline.
    Runs 4 scripts in order: fetch → process → chain → IV
    """
    def __init__(self, interval: int = 300):
        self.interval = interval  # seconds
        self.thread: threading.Thread = None
        self.running = False
        self.last_run_start: float = 0.0
        self.last_update: str = None
        self.status: str = "Idle"
        self.lock = threading.Lock()
        self._next_run_time: float = 0.0

        # Define pipeline steps: (name, relative_path)
        self.steps: List[Tuple[str, str]] = [
            ("data_fetcher", "Scripts/data_fetcher.py"),
            ("date_processor", "Scripts/date_processor.py"),
            ("chain_calculator", "Scripts/chain_calculator.py"),
            ("iv_calculator", "Scripts/iv_calculator.py")
        ]

    def _get_abs_path(self, rel_path: str) -> str:
        """Convert relative path to absolute from project root"""
        base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        return os.path.join(base_dir, rel_path)

    def _load_module(self, name: str, path: str):
        """Dynamically load a module from file"""
        abs_path = self._get_abs_path(path)
        if not os.path.exists(abs_path):
            raise FileNotFoundError(f"Script not found: {abs_path}")

        try:
            spec = importlib.util.spec_from_file_location(name, abs_path)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            return module
        except Exception as e:
            raise ImportError(f"Failed to load module {name}: {e}")

    def _run_step(self, name: str, path: str) -> bool:
        """Run one script and return success"""
        abs_path = self._get_abs_path(path)
        timestamp = datetime.now().strftime("%H:%M:%S")
        print(f" [{timestamp}] → Running {name}... ({abs_path})")
        try:
            module = self._load_module(name, path)
            if not hasattr(module, 'main'):
                print(f" [WARN] {name} has no main() function")
                return True  # Not an error, just a warning
            module.main()
            print(f" [OK] {name} completed successfully")
            return True
        except Exception as e:
            print(f" [ERROR] {name} failed: {e}")
            traceback.print_exc()
            return False

    def _run_pipeline(self) -> None:
        """Run all steps in order"""
        start_time = time.time()
        timestamp = datetime.now().strftime("%Y/%m/%d %H:%M:%S")
        print(f"\n{'='*60}")
        print(f" PIPELINE STARTED @ {timestamp}")
        print(f"{'='*60}")

        with self.lock:
            self.status = "Running..."
            self.last_run_start = start_time

        success_count = 0
        total = len(self.steps)

        for name, path in self.steps:
            if self._run_step(name, path):
                success_count += 1
            else:
                # Continue to show all errors
                pass

        # Final status
        end_time = time.time()
        duration = int(end_time - start_time)
        now = datetime.now().strftime("%Y/%m/%d %H:%M:%S")

        with self.lock:
            self.last_update = now
            self.status = f"Success ({success_count}/{total})" if success_count == total else f"Failed ({success_count}/{total})"
            self._next_run_time = end_time + self.interval

        print(f"{'='*60}")
        print(f" PIPELINE FINISHED @ {now} | Duration: {duration}s")
        print(f" Status: {self.status}")
        print(f" Next run in {self.interval}s")
        print(f"{'='*60}\n")

    def start(self) -> None:
        """Start the background updater"""
        if self.running:
            print("[UPDATER] Already running")
            return

        self.running = True
        self._next_run_time = time.time()  # Run immediately
        self.thread = threading.Thread(target=self._updater_loop, daemon=True)
        self.thread.start()
        print(f"[UPDATER] Started | Interval: {self.interval}s | First run: now")

    def stop(self) -> None:
        """Stop the updater"""
        if not self.running:
            return

        self.running = False
        if self.thread:
            self.thread.join(timeout=15)
        print("[UPDATER] Stopped")

    def _updater_loop(self) -> None:
        """Main loop: run pipeline on schedule"""
        while self.running:
            current_time = time.time()
            if current_time >= self._next_run_time:
                self._run_pipeline()
                # Schedule next run
                with self.lock:
                    self._next_run_time = time.time() + self.interval
            else:
                # Sleep until next run
                sleep_time = max(1, self._next_run_time - current_time)
                time.sleep(sleep_time)

    def get_status(self) -> Dict[str, Any]:
        """Return current status for API"""
        with self.lock:
            now = time.time()
            next_in = max(0, int(self._next_run_time - now)) if self._next_run_time > 0 else self.interval
            return {
                "status": self.status,
                "last_update": self.last_update or "Never",
                "next_update_in_seconds": next_in,
                "interval_seconds": self.interval
            }