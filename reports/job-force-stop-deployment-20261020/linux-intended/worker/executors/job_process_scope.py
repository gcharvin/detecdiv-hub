"""OS containment for one execution; never select processes by name or stored PID."""

from __future__ import annotations

import ctypes
import logging
import os
import signal
import subprocess
import sys
import time
from ctypes import wintypes
from pathlib import Path

import psutil

LOGGER = logging.getLogger(__name__)


class JobProcessScope:
    def __init__(self):
        self.process = None
        self.root = None
        self._windows = WindowsJob() if os.name == "nt" else None

    def start(self, command, **kwargs):
        if self._windows:
            # Contain the child before it can execute or spawn any descendants.
            self.process = subprocess.Popen(
                command, creationflags=0x4, **kwargs
            )  # CREATE_SUSPENDED
            try:
                self._windows.assign_and_resume(self.process)
            except BaseException:
                self.process.kill()
                self.process.wait()
                raise
        else:
            if not sys.platform.startswith("linux"):
                raise RuntimeError("Forced-stop containment requires Linux or Windows")
            supervisor = Path(__file__).with_name("linux_job_supervisor.py")
            read_fd, write_fd = os.pipe()
            try:
                self.process = subprocess.Popen(
                    [sys.executable, str(supervisor), str(write_fd), *command],
                    pass_fds=(write_fd,),
                    start_new_session=True,
                    **kwargs,
                )
            except BaseException:
                os.close(read_fd)
                raise
            finally:
                os.close(write_fd)
            try:
                ready = os.read(read_fd, 1)
            finally:
                os.close(read_fd)
            if ready != b"1":
                self.process.wait()
                raise RuntimeError(
                    "Job supervisor failed before launching the command; inspect stderr"
                )
        try:
            self.root = psutil.Process(self.process.pid)
        except psutil.NoSuchProcess:
            pass  # A short successful command can already be finished.
        return self.process

    def running(self):
        if self.process is None:
            return False
        root_running = self.process.poll() is None
        return bool(self._windows.active_count()) if self._windows else root_running

    def kill(self):
        if self.process is None:
            return
        # Keep the worker/job busy until the OS confirms that containment is empty.
        # Uninterruptible kernel I/O may delay this; never free locks prematurely.
        while self.running():
            try:
                if self._windows:
                    self._windows.terminate()
                else:
                    self._kill_linux_children()
            except (OSError, psutil.Error):
                if self.running():
                    LOGGER.exception(
                        "Job process cleanup pending; retaining worker and resource reservations"
                    )
                    time.sleep(1)
            time.sleep(0.05)
        self.process.wait()

    def _kill_linux_children(self):
        # The private subreaper only waits after launching MATLAB. Freeze parents
        # before traversing children so forks cannot race the kill. Detached and
        # double-forked children are adopted by this supervisor, not the worker.
        while True:
            children = self.root.children(recursive=True)
            unstopped = False
            for child in children:
                try:
                    if child.status() not in {psutil.STATUS_STOPPED, psutil.STATUS_ZOMBIE}:
                        child.send_signal(signal.SIGSTOP)
                        unstopped = True
                except psutil.NoSuchProcess:
                    pass
            if not unstopped:
                break
            time.sleep(0.01)
        for child in reversed(children):
            try:
                child.kill()
            except psutil.NoSuchProcess:
                pass

    def close(self):
        try:
            if self.running():
                self.kill()
        finally:
            if self._windows:
                self._windows.close()


class WindowsJob:
    """A private, non-breakaway Job Object, including parallel workers and Python."""

    def __init__(self):
        self.api = ctypes.WinDLL("kernel32", use_last_error=True)
        signatures = {
            "CreateJobObjectW": ([ctypes.c_void_p, wintypes.LPCWSTR], wintypes.HANDLE),
            "SetInformationJobObject": (
                [wintypes.HANDLE, ctypes.c_int, ctypes.c_void_p, wintypes.DWORD],
                wintypes.BOOL,
            ),
            "QueryInformationJobObject": (
                [wintypes.HANDLE, ctypes.c_int, ctypes.c_void_p, wintypes.DWORD, ctypes.c_void_p],
                wintypes.BOOL,
            ),
            "AssignProcessToJobObject": ([wintypes.HANDLE, wintypes.HANDLE], wintypes.BOOL),
            "TerminateJobObject": ([wintypes.HANDLE, wintypes.UINT], wintypes.BOOL),
            "OpenThread": ([wintypes.DWORD, wintypes.BOOL, wintypes.DWORD], wintypes.HANDLE),
            "ResumeThread": ([wintypes.HANDLE], wintypes.DWORD),
            "CloseHandle": ([wintypes.HANDLE], wintypes.BOOL),
        }
        for name, (args, result) in signatures.items():
            getattr(self.api, name).argtypes = args
            getattr(self.api, name).restype = result
        self.handle = self.api.CreateJobObjectW(None, None)
        self._check(self.handle)

        class BasicLimits(ctypes.Structure):
            _fields_ = [
                ("process_time", ctypes.c_int64),
                ("job_time", ctypes.c_int64),
                ("flags", wintypes.DWORD),
                ("min_ws", ctypes.c_size_t),
                ("max_ws", ctypes.c_size_t),
                ("active_limit", wintypes.DWORD),
                ("affinity", ctypes.c_size_t),
                ("priority", wintypes.DWORD),
                ("scheduling", wintypes.DWORD),
            ]

        class ExtendedLimits(ctypes.Structure):
            _fields_ = [
                ("basic", BasicLimits),
                ("io", ctypes.c_uint64 * 6),
                ("process_memory", ctypes.c_size_t),
                ("job_memory", ctypes.c_size_t),
                ("peak_process_memory", ctypes.c_size_t),
                ("peak_job_memory", ctypes.c_size_t),
            ]

        limits = ExtendedLimits()
        limits.basic.flags = 0x2000  # JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
        try:
            self._check(
                self.api.SetInformationJobObject(
                    self.handle, 9, ctypes.byref(limits), ctypes.sizeof(limits)
                )
            )
        except BaseException:
            self.close()
            raise

    @staticmethod
    def _check(result):
        if not result:
            raise ctypes.WinError(ctypes.get_last_error())

    def assign_and_resume(self, process):
        self._check(self.api.AssignProcessToJobObject(self.handle, int(process._handle)))
        threads = psutil.Process(process.pid).threads()
        if len(threads) != 1:
            raise RuntimeError("Suspended job has an unexpected thread count")
        thread = self.api.OpenThread(0x2, False, threads[0].id)  # THREAD_SUSPEND_RESUME
        self._check(thread)
        try:
            if self.api.ResumeThread(thread) == 0xFFFFFFFF:
                raise ctypes.WinError(ctypes.get_last_error())
        finally:
            self.api.CloseHandle(thread)

    def active_count(self):
        class Accounting(ctypes.Structure):
            _fields_ = [
                ("times", ctypes.c_int64 * 4),
                ("faults", wintypes.DWORD),
                ("total", wintypes.DWORD),
                ("active", wintypes.DWORD),
                ("terminated", wintypes.DWORD),
            ]

        info = Accounting()
        self._check(
            self.api.QueryInformationJobObject(
                self.handle, 1, ctypes.byref(info), ctypes.sizeof(info), None
            )
        )
        return info.active

    def terminate(self):
        self._check(self.api.TerminateJobObject(self.handle, 137))

    def close(self):
        if self.handle:
            self.api.CloseHandle(self.handle)
            self.handle = None
