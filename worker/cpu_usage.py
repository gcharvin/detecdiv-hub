from __future__ import annotations

import os
import threading
import time
from typing import Any

import psutil


def get_cpu_topology() -> tuple[int | None, int | None]:
    """Return host logical CPUs and CPUs available through this process affinity."""
    host_cpu_count = psutil.cpu_count(logical=True) or os.cpu_count()
    available_cpu_count = None
    try:
        affinity = psutil.Process().cpu_affinity()
        if affinity:
            available_cpu_count = len(affinity)
    except (AttributeError, OSError, psutil.Error):
        pass
    if available_cpu_count is None:
        available_cpu_count = host_cpu_count
    return host_cpu_count, available_cpu_count


class JobCpuMonitor:
    """Sample CPU time used by the worker process and its child-process tree."""

    def __init__(self, *, interval_seconds: float = 0.5) -> None:
        self.interval_seconds = max(0.25, float(interval_seconds))
        self.host_cpu_count, self.available_cpu_count = get_cpu_topology()
        self._root = psutil.Process(os.getpid())
        self._started_at = time.monotonic()
        self._started_wall_time = time.time()
        self._last_sample_at = self._started_at
        self._previous_cpu_by_process = self._capture_process_tree()
        self._cpu_seconds = 0.0
        self._current_cores = 0.0
        self._peak_cores = 0.0
        self._sample_count = 0
        self._lock = threading.Lock()
        self._finish_lock = threading.Lock()
        self._finished_metrics: dict[str, Any] | None = None
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    @property
    def current_cores(self) -> float:
        with self._lock:
            return self._current_cores

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(
            target=self._sample_loop,
            name="job-cpu-monitor",
            daemon=True,
        )
        self._thread.start()

    def finish(self) -> dict[str, Any]:
        with self._finish_lock:
            if self._finished_metrics is not None:
                return dict(self._finished_metrics)
            self._stop_event.set()
            if self._thread is not None:
                self._thread.join(timeout=max(2.0, self.interval_seconds * 3))
            self._sample()
            with self._lock:
                wall_seconds = max(0.0, time.monotonic() - self._started_at)
                cpu_seconds = max(0.0, self._cpu_seconds)
                self._finished_metrics = {
                    "host_cpu_count": self.host_cpu_count,
                    "available_cpu_count": self.available_cpu_count,
                    "cpu_seconds": round(cpu_seconds, 3),
                    "wall_seconds": round(wall_seconds, 3),
                    "average_cores": round(cpu_seconds / wall_seconds, 3) if wall_seconds else 0.0,
                    "peak_cores": round(self._peak_cores, 3),
                    "sample_count": self._sample_count,
                    "sample_interval_seconds": self.interval_seconds,
                }
            return dict(self._finished_metrics)

    def _sample_loop(self) -> None:
        while not self._stop_event.wait(self.interval_seconds):
            self._sample()

    def _capture_process_tree(self) -> dict[tuple[int, float], float]:
        try:
            processes = [self._root, *self._root.children(recursive=True)]
        except (psutil.Error, OSError):
            processes = [self._root]

        cpu_by_process: dict[tuple[int, float], float] = {}
        for process in processes:
            try:
                identity = (process.pid, process.create_time())
                times = process.cpu_times()
                cpu_by_process[identity] = max(0.0, times.user + times.system)
            except (psutil.Error, OSError):
                continue
        return cpu_by_process

    def _sample(self) -> None:
        now = time.monotonic()
        current_cpu_by_process = self._capture_process_tree()
        with self._lock:
            elapsed = max(0.0, now - self._last_sample_at)
            if elapsed <= 0:
                return
            delta_cpu_seconds = 0.0
            for identity, cpu_seconds in current_cpu_by_process.items():
                previous = self._previous_cpu_by_process.get(identity)
                if previous is not None:
                    delta_cpu_seconds += max(0.0, cpu_seconds - previous)
                elif identity[1] >= self._started_wall_time:
                    # New descendants started by this job report their full CPU
                    # time at first observation, including the initial sample gap.
                    delta_cpu_seconds += cpu_seconds
            self._previous_cpu_by_process = current_cpu_by_process
            self._last_sample_at = now
            self._cpu_seconds += delta_cpu_seconds
            self._current_cores = delta_cpu_seconds / elapsed
            self._peak_cores = max(self._peak_cores, self._current_cores)
            self._sample_count += 1


def merge_cpu_usage(previous: object, current: object) -> dict[str, Any] | None:
    """Aggregate CPU measurements across resumable/chunked executions of a job."""
    old = dict(previous) if isinstance(previous, dict) else None
    new = dict(current) if isinstance(current, dict) else None
    if old is None:
        return new
    if new is None:
        return old

    cpu_seconds = _nonnegative_float(old.get("cpu_seconds")) + _nonnegative_float(new.get("cpu_seconds"))
    wall_seconds = _nonnegative_float(old.get("wall_seconds")) + _nonnegative_float(new.get("wall_seconds"))
    merged = {**old, **new}
    merged.update(
        {
            "cpu_seconds": round(cpu_seconds, 3),
            "wall_seconds": round(wall_seconds, 3),
            "average_cores": round(cpu_seconds / wall_seconds, 3) if wall_seconds else 0.0,
            "peak_cores": round(
                max(_nonnegative_float(old.get("peak_cores")), _nonnegative_float(new.get("peak_cores"))),
                3,
            ),
            "sample_count": int(old.get("sample_count") or 0) + int(new.get("sample_count") or 0),
        }
    )
    return merged


def _nonnegative_float(value: object) -> float:
    try:
        return max(0.0, float(value))
    except (TypeError, ValueError):
        return 0.0
