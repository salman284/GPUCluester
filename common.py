"""
common.py  -  Master ar Worker dutor-i use kore (duto PC-te-i thakbe)
 - password hash, system specs (IP, CPU, RAM, GPU), compute engine (GPU/CPU)
"""
import hashlib
import hmac
import json
import os
import platform
import secrets
import socket
import sqlite3
import threading
import urllib.request

import numpy as np

DEFAULT_MASTER_PORT = 8000


# ---------------------------------------------------------------- passwords
def hash_password(password, salt=None):
    salt = salt or secrets.token_hex(16)
    h = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), bytes.fromhex(salt), 100_000)
    return salt, h.hex()


def check_password(password, salt, hashed):
    _, h = hash_password(password, salt)
    return hmac.compare_digest(h, hashed)


# ---------------------------------------------------------------- tiny sqlite wrapper
class DB:
    """Thread-safe sqlite wrapper (ekta connection + lock)."""

    def __init__(self, path):
        self.lock = threading.RLock()
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row

    def run(self, sql, args=()):
        with self.lock:
            cur = self.conn.execute(sql, args)
            self.conn.commit()
            return cur

    def all(self, sql, args=()):
        with self.lock:
            return [dict(r) for r in self.conn.execute(sql, args).fetchall()]

    def one(self, sql, args=()):
        with self.lock:
            r = self.conn.execute(sql, args).fetchone()
            return dict(r) if r else None


# ---------------------------------------------------------------- system specs
def local_ips():
    ips = set()
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("10.255.255.255", 1))  # kichu pathay na, shudhu route dekhe
        ips.add(s.getsockname()[0])
        s.close()
    except Exception:
        pass
    try:
        for ip in socket.gethostbyname_ex(socket.gethostname())[2]:
            if not ip.startswith("127."):
                ips.add(ip)
    except Exception:
        pass
    return sorted(ips)


def ram_gb():
    try:
        if os.name == "nt":
            import ctypes

            class MEM(ctypes.Structure):
                _fields_ = [("dwLength", ctypes.c_ulong), ("dwMemoryLoad", ctypes.c_ulong),
                            ("ullTotalPhys", ctypes.c_ulonglong), ("ullAvailPhys", ctypes.c_ulonglong),
                            ("ullTotalPageFile", ctypes.c_ulonglong), ("ullAvailPageFile", ctypes.c_ulonglong),
                            ("ullTotalVirtual", ctypes.c_ulonglong), ("ullAvailVirtual", ctypes.c_ulonglong),
                            ("sullAvailExtendedVirtual", ctypes.c_ulonglong)]
            m = MEM()
            m.dwLength = ctypes.sizeof(MEM)
            ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(m))
            return round(m.ullTotalPhys / 1024 ** 3, 1)
        with open("/proc/meminfo") as f:
            for line in f:
                if line.startswith("MemTotal"):
                    return round(int(line.split()[1]) / 1024 ** 2, 1)
    except Exception:
        pass
    return 0.0


def cpu_name():
    name = os.environ.get("PROCESSOR_IDENTIFIER") or platform.processor()
    if not name:
        try:
            with open("/proc/cpuinfo") as f:
                for line in f:
                    if line.startswith("model name"):
                        name = line.split(":", 1)[1].strip()
                        break
        except Exception:
            pass
    return name or "unknown"


def list_opencl_devices():
    """Return [(platform, device, global_mem_gb, type)] ; pyopencl na thakle empty."""
    out = []
    try:
        import pyopencl as cl
        for p in cl.get_platforms():
            for d in p.get_devices():
                out.append({
                    "platform": p.name.strip(),
                    "device": d.name.strip(),
                    "mem_gb": round(d.global_mem_size / 1024 ** 3, 1),
                    "is_gpu": bool(d.type & cl.device_type.GPU),
                })
    except Exception:
        pass
    return out


def system_specs():
    devs = list_opencl_devices()
    gpus = [d for d in devs if d["is_gpu"]]
    gpu_text = ", ".join(f"{d['device']} ({d['mem_gb']} GB)" for d in gpus) if gpus else "GPU not detected (OpenCL)"
    return {
        "hostname": socket.gethostname(),
        "ips": local_ips(),
        "os": f"{platform.system()} {platform.release()}",
        "python": platform.python_version(),
        "cpu": cpu_name(),
        "cpu_cores": os.cpu_count() or 0,
        "ram_gb": ram_gb(),
        "gpu": gpu_text,
        "opencl_gpu_count": len(gpus),
    }


# ---------------------------------------------------------------- compute engines
KERNEL = """
__kernel void poly(__global const float *x, __global float *y) {
    int i = get_global_id(0);
    y[i] = x[i]*x[i] + 2.0f*x[i] + 1.0f;
}
"""


def make_input(task_no, n):
    """Deterministic input -- master o same data banay validation-er jonno."""
    return np.random.RandomState(task_no).rand(n).astype(np.float32)


def expected_value(task_no, n):
    x = make_input(task_no, n).astype(np.float64)
    return float(np.sum(x * x + 2.0 * x + 1.0))


class Engine:
    """Ekta compute engine. Pehle OpenCL GPU, na pele CPU (numpy)."""

    def __init__(self, prefer_gpu=True):
        self.kind = "CPU"
        self.label = "CPU (numpy)"
        self.error = None
        self._cl = None
        if prefer_gpu:
            try:
                import pyopencl as cl
                best = None
                for p in cl.get_platforms():
                    for d in p.get_devices():
                        if d.type & cl.device_type.GPU:
                            best = best or (p, d)
                if best:
                    p, d = best
                    ctx = cl.Context([d])
                    q = cl.CommandQueue(ctx)
                    prg = cl.Program(ctx, KERNEL).build()
                    self._cl = (cl, ctx, q, prg)
                    self.kind = "GPU"
                    self.label = f"GPU: {d.name.strip()} [{p.name.strip()}]"
                    # self-test: GPU sotti kaj kore kina
                    got = self._run_gpu(1, 1000)
                    if abs(got - expected_value(1, 1000)) > 1e-3 * expected_value(1, 1000):
                        raise RuntimeError("GPU self-test result mismatch")
                else:
                    self.error = "OpenCL GPU device paoa jayni"
            except Exception as e:  # noqa
                self._cl = None
                self.kind = "CPU"
                self.label = "CPU (numpy)"
                self.error = f"GPU use kora gelo na: {e}"

    def _run_gpu(self, task_no, n):
        cl, ctx, q, prg = self._cl
        x = make_input(task_no, n)
        y = np.empty_like(x)
        mf = cl.mem_flags
        xb = cl.Buffer(ctx, mf.READ_ONLY | mf.COPY_HOST_PTR, hostbuf=x)
        yb = cl.Buffer(ctx, mf.WRITE_ONLY, y.nbytes)
        prg.poly(q, (n,), None, xb, yb)
        cl.enqueue_copy(q, y, yb)
        q.finish()
        return float(np.sum(y.astype(np.float64)))

    def run(self, task_no, n):
        """Return (value, engine_label)."""
        if self._cl:
            return self._run_gpu(task_no, n), self.label
        x = make_input(task_no, n)
        return float(np.sum((x * x + 2.0 * x + 1.0).astype(np.float64))), self.label


# ARC-AGI integration point: porer dhape ei function-er bhitore ARC solver boshabe.
def run_task(engine, payload):
    value, label = engine.run(payload["task_no"], payload["n"])
    return {"value": value, "engine": label}


# ---------------------------------------------------------------- http json helper (worker -> master)
def post_json(url, data, timeout=30):
    req = urllib.request.Request(url, data=json.dumps(data).encode("utf-8"),
                                 headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8"))
