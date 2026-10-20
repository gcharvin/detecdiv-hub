"""Private Linux launcher: keep even detached job descendants in one process tree.

The worker can stop that tree without becoming a subreaper for other jobs.
This file is executed directly by the worker's Python interpreter.
"""

import ctypes
import os
import subprocess
import sys


def main():
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(36, 1, 0, 0, 0) != 0:  # PR_SET_CHILD_SUBREAPER
        raise OSError(ctypes.get_errno(), "Cannot isolate job descendants")
    ready_fd = int(sys.argv[1])
    os.set_inheritable(ready_fd, False)
    process = subprocess.Popen(sys.argv[2:])
    os.write(ready_fd, b"1")
    os.close(ready_fd)
    # Reap the main process AND adopted descendants before reporting completion.
    returncode = 1
    while True:
        try:
            pid, status = os.waitpid(-1, 0)
        except ChildProcessError:
            break
        if pid == process.pid:
            returncode = os.waitstatus_to_exitcode(status)
            process.returncode = returncode
    return returncode if returncode >= 0 else 128 - returncode


if __name__ == "__main__":
    sys.exit(main())
