"""
master.py  -  MASTER PC-te chalate hobe.
 * Browser-e dashboard khole (http://localhost:8000) admin user id/password diye dhukben
 * Worker PC-ra register korbe -> apni Approve kore dile tader kaj dewa hobe
 * Master PC nijeo ekta node ("MASTER") hishebe kaj kore (jodi enabled thake)
 * Alada database: master.db
"""
import argparse
import csv
import io
import json
import os
import secrets
import sys
import threading
import time
import webbrowser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from http.cookies import SimpleCookie

from common import (DB, DEFAULT_MASTER_PORT, Engine, check_password, expected_value,
                    hash_password, run_task, system_specs)

LEASE_SECONDS = 120      # worker eto second-er moddhe result na dile task abar queue-te jabe
MAX_ATTEMPTS = 5
ONLINE_SECONDS = 15
MASTER_NODE_ID = "MASTER"

DB_PATH = "master.db"
db = None
sessions = {}            # cookie token -> username
auth_cache = {}          # (node_id, password) -> True  (pbkdf2 bar bar na chalanor jonno)


# ================================================================= database
def init_db(path):
    global db
    db = DB(path)
    db.run("""CREATE TABLE IF NOT EXISTS users(
        username TEXT PRIMARY KEY, salt TEXT, hash TEXT)""")
    db.run("""CREATE TABLE IF NOT EXISTS nodes(
        node_id TEXT PRIMARY KEY, salt TEXT, hash TEXT, is_master INTEGER DEFAULT 0,
        status TEXT DEFAULT 'pending', ip TEXT, hostname TEXT, cpu TEXT, cpu_cores INTEGER,
        ram_gb REAL, gpu TEXT, os TEXT, specs_json TEXT, engine TEXT,
        registered_at REAL, last_seen REAL)""")
    db.run("""CREATE TABLE IF NOT EXISTS jobs(
        id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, total INTEGER, n INTEGER,
        status TEXT, created REAL, finished REAL)""")
    db.run("""CREATE TABLE IF NOT EXISTS tasks(
        id INTEGER PRIMARY KEY AUTOINCREMENT, job_id INTEGER, task_no INTEGER,
        status TEXT DEFAULT 'queued', node_id TEXT, lease_until REAL, attempts INTEGER DEFAULT 0,
        value REAL, engine TEXT, ms REAL, done_at REAL)""")
    db.run("CREATE INDEX IF NOT EXISTS idx_tasks ON tasks(job_id, status)")
    # server restart holei ager leased task gulo queue-te fire jak
    db.run("UPDATE tasks SET status='queued', node_id=NULL, lease_until=NULL WHERE status='leased'")


def upsert_master_node():
    specs = system_specs()
    row = db.one("SELECT node_id FROM nodes WHERE node_id=?", (MASTER_NODE_ID,))
    vals = (specs["ips"][0] if specs["ips"] else "127.0.0.1", specs["hostname"], specs["cpu"],
            specs["cpu_cores"], specs["ram_gb"], specs["gpu"], specs["os"], json.dumps(specs))
    if row:
        db.run("""UPDATE nodes SET ip=?,hostname=?,cpu=?,cpu_cores=?,ram_gb=?,gpu=?,os=?,specs_json=?
                  WHERE node_id=?""", vals + (MASTER_NODE_ID,))
    else:
        db.run("""INSERT INTO nodes(node_id,is_master,status,ip,hostname,cpu,cpu_cores,ram_gb,gpu,os,
                  specs_json,registered_at,last_seen) VALUES(?,1,'approved',?,?,?,?,?,?,?,?,?,?)""",
               (MASTER_NODE_ID,) + vals + (time.time(), time.time()))


# ================================================================= scheduler
def requeue_expired():
    now = time.time()
    db.run("UPDATE tasks SET status='queued', node_id=NULL, lease_until=NULL "
           "WHERE status='leased' AND lease_until<?", (now,))


def running_job():
    return db.one("SELECT * FROM jobs WHERE status='running' ORDER BY id DESC LIMIT 1")


def finish_job_if_done(job_id):
    c = db.one("""SELECT SUM(status='done') d, SUM(status='failed') f, COUNT(*) t FROM tasks
                  WHERE job_id=?""", (job_id,))
    if (c["d"] or 0) + (c["f"] or 0) == c["t"]:
        status = "completed" if c["d"] == c["t"] else "completed_with_failures"
        db.run("UPDATE jobs SET status=?, finished=? WHERE id=?", (status, time.time(), job_id))


def next_task(node_id):
    """Ei node-ke ekta unique task dao. Age theke leased thakle sheta-i abar dey (idempotent)."""
    with db.lock:
        node = db.one("SELECT status FROM nodes WHERE node_id=?", (node_id,))
        if not node or node["status"] != "approved":
            return None
        job = running_job()
        if not job:
            return None
        requeue_expired()
        now = time.time()
        t = db.one("SELECT * FROM tasks WHERE job_id=? AND status='leased' AND node_id=? LIMIT 1",
                   (job["id"], node_id))
        if not t:
            t = db.one("SELECT * FROM tasks WHERE job_id=? AND status='queued' "
                       "ORDER BY task_no LIMIT 1", (job["id"],))
            if not t:
                return None
        db.run("UPDATE tasks SET status='leased', node_id=?, lease_until=?, attempts=attempts+1 "
               "WHERE id=?", (node_id, now + LEASE_SECONDS, t["id"]))
        return {"task_id": t["id"], "task_no": t["task_no"], "n": job["n"], "job_id": job["id"]}


def submit_result(node_id, task_id, value, engine, ms):
    """Return 'accepted' | 'duplicate' | 'rejected'. Ekta task-er result shudhu ekbar accept hoy."""
    with db.lock:
        t = db.one("SELECT t.*, j.n FROM tasks t JOIN jobs j ON j.id=t.job_id WHERE t.id=?", (task_id,))
        if not t:
            return "rejected"
        if t["status"] == "done":
            return "duplicate"
        exp = expected_value(t["task_no"], t["n"])
        if value is None or abs(value - exp) > 1e-3 * abs(exp):
            if t["attempts"] >= MAX_ATTEMPTS:
                db.run("UPDATE tasks SET status='failed' WHERE id=?", (task_id,))
                finish_job_if_done(t["job_id"])
            else:
                db.run("UPDATE tasks SET status='queued', node_id=NULL, lease_until=NULL WHERE id=?",
                       (task_id,))
            return "rejected"
        db.run("UPDATE tasks SET status='done', node_id=?, value=?, engine=?, ms=?, done_at=? WHERE id=?",
               (node_id, value, engine, ms, time.time(), task_id))
        finish_job_if_done(t["job_id"])
        return "accepted"


def start_job(name, total, n):
    with db.lock:
        if running_job():
            return None, "Ekta job already cholche"
        cur = db.run("INSERT INTO jobs(name,total,n,status,created) VALUES(?,?,?,'running',?)",
                     (name, total, n, time.time()))
        jid = cur.lastrowid
        for i in range(1, total + 1):
            db.conn.execute("INSERT INTO tasks(job_id,task_no) VALUES(?,?)", (jid, i))
        db.conn.commit()
        return jid, None


# ================================================================= master's own node
def local_node_loop(stop_event, prefer_gpu=True):
    engine = Engine(prefer_gpu)
    db.run("UPDATE nodes SET engine=? WHERE node_id=?", (engine.label, MASTER_NODE_ID))
    print(f"[MASTER-node] engine: {engine.label}" + (f"  ({engine.error})" if engine.error else ""))
    while not stop_event.is_set():
        db.run("UPDATE nodes SET last_seen=? WHERE node_id=?", (time.time(), MASTER_NODE_ID))
        t = next_task(MASTER_NODE_ID)
        if not t:
            time.sleep(1)
            continue
        t0 = time.time()
        try:
            r = run_task(engine, {"task_no": t["task_no"], "n": t["n"]})
            submit_result(MASTER_NODE_ID, t["task_id"], r["value"], r["engine"],
                          (time.time() - t0) * 1000)
        except Exception as e:
            print("[MASTER-node] task error:", e)
            time.sleep(1)


# ================================================================= state for dashboard
def build_state():
    now = time.time()
    nodes = db.all("SELECT * FROM nodes ORDER BY is_master DESC, registered_at")
    job = db.one("SELECT * FROM jobs ORDER BY id DESC LIMIT 1")
    per_node = {}
    prog = None
    recent = []
    if job:
        for r in db.all("""SELECT node_id, COUNT(*) c, AVG(ms) a FROM tasks
                           WHERE job_id=? AND status='done' GROUP BY node_id""", (job["id"],)):
            per_node[r["node_id"]] = (r["c"], r["a"])
        c = db.one("""SELECT SUM(status='done') d, SUM(status='leased') l, SUM(status='queued') q,
                      SUM(status='failed') f FROM tasks WHERE job_id=?""", (job["id"],))
        prog = {k: (c[k] or 0) for k in ("d", "l", "q", "f")}
        recent = db.all("""SELECT task_no, node_id, value, engine, ms FROM tasks WHERE job_id=?
                           AND status='done' ORDER BY done_at DESC LIMIT 15""", (job["id"],))
    out_nodes = []
    for n in nodes:
        done, avg = per_node.get(n["node_id"], (0, None))
        out_nodes.append({
            "node_id": n["node_id"], "is_master": bool(n["is_master"]), "status": n["status"],
            "online": (now - (n["last_seen"] or 0)) < ONLINE_SECONDS, "ip": n["ip"],
            "hostname": n["hostname"], "cpu": n["cpu"], "cpu_cores": n["cpu_cores"],
            "ram_gb": n["ram_gb"], "gpu": n["gpu"], "os": n["os"], "engine": n["engine"],
            "done": done, "avg_ms": round(avg, 1) if avg else None,
        })
    jinfo = None
    if job:
        end = job["finished"] or now
        jinfo = {"id": job["id"], "name": job["name"], "total": job["total"], "n": job["n"],
                 "status": job["status"], "elapsed": round(end - job["created"], 1)}
    return {"nodes": out_nodes, "job": jinfo, "progress": prog, "recent": recent}


# ================================================================= HTTP
class Handler(BaseHTTPRequestHandler):
    server_version = "ARCMaster/1.0"

    def log_message(self, *a):
        pass

    # ---- helpers
    def send_json(self, obj, code=200, headers=None):
        body = json.dumps(obj).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        for k, v in (headers or {}).items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(body)

    def read_json(self):
        n = int(self.headers.get("Content-Length") or 0)
        if n > 1_000_000:
            return {}
        try:
            return json.loads(self.rfile.read(n).decode("utf-8") or "{}")
        except Exception:
            return {}

    def user(self):
        c = SimpleCookie(self.headers.get("Cookie", ""))
        return sessions.get(c["sid"].value) if "sid" in c else None

    def node_auth(self, d):
        nid, pw = d.get("node_id", ""), d.get("password", "")
        node = db.one("SELECT * FROM nodes WHERE node_id=? AND is_master=0", (nid,))
        if not node:
            return None
        if auth_cache.get((nid, pw)):
            return node
        if check_password(pw, node["salt"], node["hash"]):
            auth_cache[(nid, pw)] = True
            return node
        time.sleep(0.5)
        return None

    # ---- GET
    def do_GET(self):
        if self.path == "/" or self.path.startswith("/?"):
            body = DASHBOARD_HTML.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        elif self.path == "/api/me":
            who = self.user()
            self.send_json({"user": who.split(":", 1)[1] if who else None,
                            "role": who.split(":", 1)[0] if who else None,
                            "setup_needed": db.one("SELECT 1 x FROM users") is None})
        elif self.path == "/api/state":
            if not self.user() or not self.user().startswith("admin:"):
                return self.send_json({"error": "login lagbe"}, 401)
            self.send_json(build_state())
        elif self.path == "/api/worker/me":
            who = self.user()
            if not who or not who.startswith("worker:"):
                return self.send_json({"error": "worker login lagbe"}, 401)
            node = db.one("SELECT node_id,status,ip,hostname,engine,last_seen FROM nodes WHERE node_id=? AND is_master=0",
                          (who.split(":", 1)[1],))
            if not node:
                return self.send_json({"error": "Worker account pawa jay ni"}, 404)
            self.send_json(dict(node))
        elif self.path == "/api/job/export.csv":
            if not self.user() or not self.user().startswith("admin:"):
                return self.send_json({"error": "login lagbe"}, 401)
            job = db.one("SELECT * FROM jobs ORDER BY id DESC LIMIT 1")
            buf = io.StringIO()
            w = csv.writer(buf)
            w.writerow(["task_no", "status", "node", "value", "engine", "ms"])
            if job:
                for r in db.all("SELECT * FROM tasks WHERE job_id=? ORDER BY task_no", (job["id"],)):
                    w.writerow([r["task_no"], r["status"], r["node_id"], r["value"], r["engine"],
                                round(r["ms"], 1) if r["ms"] else ""])
            body = buf.getvalue().encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/csv")
            self.send_header("Content-Disposition", "attachment; filename=job_report.csv")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        else:
            self.send_json({"error": "not found"}, 404)

    # ---- POST
    def do_POST(self):
        d = self.read_json()
        p = self.path

        # ---------- worker API ----------
        if p == "/api/worker/register":
            nid, pw = str(d.get("node_id", "")).strip(), str(d.get("password", ""))
            specs = d.get("specs") or {}
            if not nid or not pw or nid == MASTER_NODE_ID or len(nid) > 40:
                return self.send_json({"ok": False, "error": "Worker ID/password thik na"}, 400)
            ip = self.client_address[0]
            existing = db.one("SELECT * FROM nodes WHERE node_id=?", (nid,))
            vals = (ip, specs.get("hostname"), specs.get("cpu"), specs.get("cpu_cores"),
                    specs.get("ram_gb"), specs.get("gpu"), specs.get("os"), json.dumps(specs),
                    time.time())
            if existing:
                if existing["is_master"] or not check_password(pw, existing["salt"], existing["hash"]):
                    time.sleep(0.5)
                    return self.send_json({"ok": False, "error": "ei Worker ID age theke ache, password mile na"}, 403)
                db.run("""UPDATE nodes SET ip=?,hostname=?,cpu=?,cpu_cores=?,ram_gb=?,gpu=?,os=?,
                          specs_json=?,last_seen=? WHERE node_id=?""", vals + (nid,))
                return self.send_json({"ok": True, "status": existing["status"], "note": "login/update hoyeche"})
            salt, h = hash_password(pw)
            db.run("""INSERT INTO nodes(node_id,salt,hash,is_master,status,ip,hostname,cpu,cpu_cores,ram_gb,
                      gpu,os,specs_json,registered_at,last_seen) VALUES(?,?,?,0,'pending',?,?,?,?,?,?,?,?,?,?)""",
                   (nid, salt, h) + vals[:-1] + (time.time(), time.time()))
            print(f"[REGISTER] new worker {nid} from {ip} -- Master dashboard theke Approve korun")
            return self.send_json({"ok": True, "status": "pending", "note": "register hoyeche, Master approve korle kaj pabe"})

        if p in ("/api/worker/task", "/api/worker/result"):
            node = self.node_auth(d)
            if not node:
                return self.send_json({"ok": False, "error": "unauthorized"}, 401)
            db.run("UPDATE nodes SET last_seen=?, ip=?, engine=COALESCE(?,engine) WHERE node_id=?",
                   (time.time(), self.client_address[0], d.get("engine"), node["node_id"]))
            if p == "/api/worker/task":
                if node["status"] != "approved":
                    return self.send_json({"ok": True, "status": node["status"], "task": None})
                return self.send_json({"ok": True, "status": "approved", "task": next_task(node["node_id"])})
            res = submit_result(node["node_id"], d.get("task_id"), d.get("value"),
                                d.get("engine"), d.get("ms"))
            return self.send_json({"ok": True, "result": res})

        # ---------- admin API ----------
        if p == "/api/setup":
            if db.one("SELECT 1 x FROM users"):
                return self.send_json({"error": "already setup"}, 403)
            u, pw = str(d.get("username", "")).strip(), str(d.get("password", ""))
            if not u or len(pw) < 4:
                return self.send_json({"error": "user id dao ar password kom-pokkhe 4 character"}, 400)
            s, h = hash_password(pw)
            db.run("INSERT INTO users VALUES(?,?,?)", (u, s, h))
            return self.login_ok(u)
        if p == "/api/login":
            u, pw = str(d.get("username", "")), str(d.get("password", ""))
            row = db.one("SELECT * FROM users WHERE username=?", (u,))
            if row and check_password(pw, row["salt"], row["hash"]):
                return self.login_ok(u)
            time.sleep(0.7)
            return self.send_json({"error": "user id ba password vul"}, 401)
        if p == "/api/worker/login":
            nid, pw = str(d.get("username", "")).strip(), str(d.get("password", ""))
            node = db.one("SELECT * FROM nodes WHERE node_id=? AND is_master=0", (nid,))
            if node and check_password(pw, node["salt"], node["hash"]):
                return self.login_ok(nid, "worker")
            time.sleep(0.5)
            return self.send_json({"error": "Worker ID ba password vul. Notun worker hole prothome worker PC-te start_worker.bat chalan."}, 401)
        if p == "/api/logout":
            c = SimpleCookie(self.headers.get("Cookie", ""))
            if "sid" in c:
                sessions.pop(c["sid"].value, None)
            return self.send_json({"ok": True})

        if not self.user() or not self.user().startswith("admin:"):
            return self.send_json({"error": "login lagbe"}, 401)

        if p == "/api/node/set":
            nid, st = d.get("node_id"), d.get("status")
            if st not in ("approved", "disabled", "pending"):
                return self.send_json({"error": "bad status"}, 400)
            db.run("UPDATE nodes SET status=? WHERE node_id=?", (st, nid))
            return self.send_json({"ok": True})
        if p == "/api/node/delete":
            nid = d.get("node_id")
            if nid == MASTER_NODE_ID:
                return self.send_json({"error": "Master node delete kora jay na (Disable korun)"}, 400)
            db.run("DELETE FROM nodes WHERE node_id=?", (nid,))
            for k in [k for k in auth_cache if k[0] == nid]:
                auth_cache.pop(k, None)
            return self.send_json({"ok": True})
        if p == "/api/job/start":
            try:
                total = max(1, min(100000, int(d.get("total", 100))))
                n = max(1000, min(50_000_000, int(d.get("n", 2_000_000))))
            except Exception:
                return self.send_json({"error": "number thik na"}, 400)
            jid, err = start_job(str(d.get("name") or "Demo GPU job")[:60], total, n)
            return self.send_json({"ok": jid is not None, "error": err, "job_id": jid})
        if p == "/api/job/stop":
            db.run("UPDATE jobs SET status='stopped', finished=? WHERE status='running'", (time.time(),))
            return self.send_json({"ok": True})
        if p == "/api/password":
            u = self.user().split(":", 1)[1]
            row = db.one("SELECT * FROM users WHERE username=?", (u,))
            if not check_password(str(d.get("old", "")), row["salt"], row["hash"]) or len(str(d.get("new", ""))) < 4:
                return self.send_json({"error": "purono password vul ba notun password chhoto"}, 400)
            s, h = hash_password(str(d["new"]))
            db.run("UPDATE users SET salt=?, hash=? WHERE username=?", (s, h, u))
            return self.send_json({"ok": True})
        self.send_json({"error": "not found"}, 404)

    def login_ok(self, username, role="admin"):
        tok = secrets.token_hex(24)
        sessions[tok] = f"{role}:{username}"
        self.send_json({"ok": True, "user": username, "role": role},
                       headers={"Set-Cookie": f"sid={tok}; Path=/; HttpOnly; SameSite=Strict"})


# ================================================================= dashboard page
DASHBOARD_HTML = r"""<!doctype html>
<html lang="bn"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>ARC Cluster - Master</title>
<style>
:root{--bg:#f5f6f8;--card:#fff;--tx:#1b1f24;--mut:#68707a;--ln:#dde1e6;--ac:#1f6feb;--ok:#1a7f37;--bad:#cf222e;--warn:#9a6700}
@media(prefers-color-scheme:dark){:root{--bg:#0f1318;--card:#171c23;--tx:#e6e9ed;--mut:#8b949e;--ln:#2b323b;--ac:#4c9aff;--ok:#3fb950;--bad:#f85149;--warn:#d29922}}
*{box-sizing:border-box}body{margin:0;font:14px/1.45 system-ui,Segoe UI,sans-serif;background:var(--bg);color:var(--tx)}
header{display:flex;justify-content:space-between;align-items:center;padding:12px 20px;background:var(--card);border-bottom:1px solid var(--ln)}
h1{font-size:17px;margin:0}main{max-width:1150px;margin:0 auto;padding:16px}
.card{background:var(--card);border:1px solid var(--ln);border-radius:10px;padding:14px 16px;margin-bottom:14px}
.card h2{font-size:14px;margin:0 0 10px;text-transform:uppercase;letter-spacing:.04em;color:var(--mut)}
table{width:100%;border-collapse:collapse}th,td{text-align:left;padding:6px 8px;border-bottom:1px solid var(--ln);vertical-align:top}
th{color:var(--mut);font-weight:600;font-size:12px}
input{padding:7px 9px;border:1px solid var(--ln);border-radius:6px;background:var(--bg);color:var(--tx);font:inherit}
button{padding:7px 13px;border:0;border-radius:6px;background:var(--ac);color:#fff;font:inherit;cursor:pointer}
button.s{background:transparent;color:var(--tx);border:1px solid var(--ln)}button.d{background:var(--bad)}button.g{background:var(--ok)}
.row{display:flex;gap:10px;flex-wrap:wrap;align-items:end}.row label{display:flex;flex-direction:column;font-size:12px;color:var(--mut);gap:3px}
.pill{display:inline-block;padding:1px 8px;border-radius:10px;font-size:12px;border:1px solid var(--ln)}
.on{color:var(--ok)}.off{color:var(--mut)}.pending{color:var(--warn)}.approved{color:var(--ok)}.disabled{color:var(--bad)}
.bar{height:16px;background:var(--bg);border:1px solid var(--ln);border-radius:8px;overflow:hidden}.bar i{display:block;height:100%;background:var(--ok);width:0;transition:width .4s}
.mut{color:var(--mut)}.login{max-width:340px;margin:12vh auto}.login input{width:100%;margin-bottom:10px}
small{color:var(--mut)}.err{color:var(--bad);min-height:1.3em}
</style></head><body>
<div id="app"></div>
<script>
const $=s=>document.querySelector(s);
const api=async(p,b)=>{const r=await fetch(p,b===undefined?{}:{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(b)});return r.json().catch(()=>({}))};
const esc=s=>String(s??'').replace(/[&<>"]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));
let timer=null;
async function boot(){
  clearInterval(timer);
  const me=await api('/api/me');
  if(!me.user){ return loginView(me.setup_needed) }
  me.role==='worker'?workerView(me.user):dashView(me.user);
}
function loginView(setup){
  let role='admin';
  $('#app').innerHTML=`<div class="card login"><h1>ARC Cluster Login</h1><p class="mut">Master ba Worker account beche nin, tarpor nijer ID ar password din.</p>
  <div class="row" style="margin-bottom:10px"><button class="s" id="adminTab">Master Admin</button><button class="s" id="workerTab">Worker</button></div>
  <p class="mut" id="hint"></p><input id="u" placeholder="User ID" autofocus><input id="p" type="password" placeholder="Password"><div class="err" id="e"></div>
  <button id="go" style="width:100%">Login</button><button id="register" class="s" style="display:none;width:100%;margin-top:8px">Register Worker</button></div>`;
  const choose=r=>{role=r;$('#adminTab').className=r==='admin'?'':'s';$('#workerTab').className=r==='worker'?'':'s';
    $('#hint').textContent=r==='admin'?(setup?'Prothom bar admin account toiri korun.':'Master dashboard-er admin credentials din.'):'Worker ID ar nijer password din. Prothom login-e Register Worker chapun. Tarpor worker PC-te start_worker.bat chaliye ei credentials din.';
    $('#register').style.display=r==='worker'?'block':'none';
    $('#go').textContent=r==='admin'&&setup?'Create Admin & Login':'Login'};
  $('#adminTab').onclick=()=>choose('admin');$('#workerTab').onclick=()=>choose('worker');choose('admin');
  const go=async()=>{const path=role==='worker'?'/api/worker/login':(setup?'/api/setup':'/api/login');const r=await api(path,{username:$('#u').value,password:$('#p').value});r.ok?boot():$('#e').textContent=r.error||'error'};
  $('#go').onclick=go;$('#p').onkeydown=e=>{if(e.key==='Enter')go()};
  $('#register').onclick=async()=>{const username=$('#u').value.trim(),password=$('#p').value;if(!username||!password){$('#e').textContent='Worker ID ar password din';return}
    const created=await api('/api/worker/register',{node_id:username,password,specs:{}});if(!created.ok){$('#e').textContent=created.error||'Register hoy ni';return}
    const logged=await api('/api/worker/login',{username,password});logged.ok?boot():$('#e').textContent=logged.error||'Login hoy ni'};
}
function workerView(id){
  $('#app').innerHTML=`<header><h1>ARC Cluster — WORKER</h1><div><b>${esc(id)}</b> &nbsp;<button class="s" id="wlo">Logout</button></div></header><main><div class="card"><h2>Worker status</h2><div id="wstate">Loading…</div></div>
  <div class="card"><h2>Connect this PC</h2><p>Worker PC-te <b>start_worker.bat</b> chalan, tarpor ei Master-er address, Worker ID, ar password diye login korun. Worker service chalu thaklei task pabe.</p></div></main>`;
  $('#wlo').onclick=async()=>{await api('/api/logout',{});boot()};
  const refreshWorker=async()=>{const s=await api('/api/worker/me');if(s.error)return boot();$('#wstate').innerHTML=`<p><b>${esc(s.node_id)}</b> — ${esc(s.status)}</p><p>PC: ${esc(s.hostname||'worker service ekhono connect hoy ni')} · ${esc(s.ip||'-')}</p><p>Engine: ${esc(s.engine||'waiting for worker service')}</p><p class="mut">Pending thakle Master admin-ke dashboard theke Approve korte bolun.</p>`};
  refreshWorker();timer=setInterval(refreshWorker,3000);
}
function dashView(user){
  $('#app').innerHTML=`<header><h1>ARC Cluster — MASTER</h1><div><span class="mut">${esc(user)}</span> &nbsp;<button class="s" id="pw">Password</button> <button class="s" id="lo">Logout</button></div></header>
  <main>
   <div class="card"><h2>Job</h2><div id="job"></div><div id="jobform"></div></div>
   <div class="card"><h2>Nodes (PC list) <small>— Approved thakle tabei kaj pabe</small></h2><div id="nodes"></div></div>
   <div class="card"><h2>Shesh kora task (latest 15)</h2><div id="recent"></div></div>
  </main>`;
  $('#lo').onclick=async()=>{await api('/api/logout',{});boot()};
  $('#pw').onclick=async()=>{const o=prompt('Purono password:');if(o===null)return;const n=prompt('Notun password (min 4):');if(n===null)return;const r=await api('/api/password',{old:o,new:n});alert(r.ok?'Password bodle geche':(r.error||'error'))};
  refresh();timer=setInterval(refresh,2000);
}
async function refresh(){
  const s=await api('/api/state'); if(s.error){return boot()}
  // ---- job
  const j=s.job,p=s.progress;let h='';
  const running=j&&j.status==='running';
  if(j){const pc=Math.round(100*p.d/j.total);
    h+=`<div class="row" style="margin-bottom:8px"><b>#${j.id} ${esc(j.name)}</b><span class="pill">${j.status}</span><span class="mut">${j.total} tasks · ${j.n.toLocaleString()} elements/task · ${j.elapsed}s</span></div>
    <div class="bar"><i style="width:${pc}%"></i></div>
    <p><b>${p.d}/${j.total}</b> done &nbsp; <span class="mut">running ${p.l} · queued ${p.q} · failed ${p.f}</span></p>`;
    if(j.status==='completed')h+=`<p class="approved"><b>✔ COMPLETE: ${p.d}/${j.total} unique valid result Master-e collect hoyeche (${j.elapsed}s).</b></p>`;
    if(j.status==='completed_with_failures')h+=`<p class="disabled"><b>Shesh hoyeche kintu ${p.f} ta task fail.</b></p>`;
    h+=`<p><a href="/api/job/export.csv"><button class="s">Report CSV download</button></a></p>`}
  $('#job').innerHTML=h;
  if(window._jr!==running||!$('#jobform').innerHTML){window._jr=running;
    $('#jobform').innerHTML=running?`<button class="d" id="jx">■ Stop job</button>`:`<div class="row"><label>Job name<input id="jn" value="Demo GPU job"></label><label>Task sonkhya<input id="jt" type="number" value="100" style="width:90px"></label><label>Elements/task<input id="jz" type="number" value="2000000" style="width:130px"></label><button class="g" id="js">▶ Start job</button></div>`;
    bindJob(running);}
  // ---- nodes
  let t=`<table><tr><th>Node</th><th>Status</th><th>IP</th><th>Host / OS</th><th>CPU</th><th>RAM</th><th>GPU</th><th>Engine</th><th>Tasks</th><th>Avg ms</th><th></th></tr>`;
  for(const n of s.nodes){
    t+=`<tr><td><b>${esc(n.node_id)}</b> ${n.is_master?'<span class="pill">Master PC</span>':''}<br><span class="${n.online?'on':'off'}">● ${n.online?'online':'offline'}</span></td>
    <td><span class="${n.status}">${n.status}</span></td><td>${esc(n.ip)}</td><td>${esc(n.hostname)}<br><small>${esc(n.os)}</small></td>
    <td>${esc(n.cpu)}<br><small>${n.cpu_cores} cores</small></td><td>${n.ram_gb} GB</td><td>${esc(n.gpu)}</td><td><small>${esc(n.engine||'-')}</small></td>
    <td><b>${n.done}</b></td><td>${n.avg_ms??'-'}</td>
    <td style="white-space:nowrap">${n.status==='approved'?`<button class="s" data-a="disabled" data-n="${esc(n.node_id)}">Disable</button>`:`<button class="g" data-a="approved" data-n="${esc(n.node_id)}">Approve</button>`}
    ${n.is_master?'':` <button class="d" data-a="del" data-n="${esc(n.node_id)}">Remove</button>`}</td></tr>`}
  $('#nodes').innerHTML=t+'</table>';
  $('#nodes').querySelectorAll('button').forEach(b=>b.onclick=async()=>{const a=b.dataset.a,n=b.dataset.n;
    if(a==='del'){if(!confirm(n+' remove korben?'))return;await api('/api/node/delete',{node_id:n})}else await api('/api/node/set',{node_id:n,status:a});refresh()});
  // ---- recent
  let r=`<table><tr><th>Task</th><th>Node</th><th>Result (sum)</th><th>Engine</th><th>ms</th></tr>`;
  for(const x of s.recent)r+=`<tr><td>TASK-${String(x.task_no).padStart(3,'0')}</td><td>${esc(x.node_id)}</td><td>${(+x.value).toFixed(2)}</td><td><small>${esc(x.engine)}</small></td><td>${Math.round(x.ms)}</td></tr>`;
  $('#recent').innerHTML=s.recent.length?r+'</table>':'<span class="mut">ekhono kono task shesh hoyni</span>';
}
function bindJob(running){
  if($('#js'))$('#js').onclick=async()=>{const r=await api('/api/job/start',{name:$('#jn').value,total:$('#jt').value,n:$('#jz').value});if(!r.ok)alert(r.error);refresh()};
  if($('#jx'))$('#jx').onclick=async()=>{await api('/api/job/stop',{});refresh()};
}
boot();
</script></body></html>
"""


# ================================================================= main
def main():
    global DB_PATH
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=DEFAULT_MASTER_PORT)
    ap.add_argument("--data-dir", default=".")
    ap.add_argument("--no-browser", action="store_true")
    ap.add_argument("--no-gpu", action="store_true", help="Master PC-te GPU use na kore CPU diye chalao")
    ap.add_argument("--admin", help="user:password  (prothom bar CLI theke admin toiri)")
    a = ap.parse_args()

    os.makedirs(a.data_dir, exist_ok=True)
    DB_PATH = os.path.join(a.data_dir, "master.db")
    init_db(DB_PATH)
    upsert_master_node()
    if a.admin and not db.one("SELECT 1 x FROM users"):
        u, pw = a.admin.split(":", 1)
        s, h = hash_password(pw)
        db.run("INSERT INTO users VALUES(?,?,?)", (u, s, h))

    stop = threading.Event()
    threading.Thread(target=local_node_loop, args=(stop, not a.no_gpu), daemon=True).start()

    srv = ThreadingHTTPServer(("0.0.0.0", a.port), Handler)
    srv.daemon_threads = True
    from common import local_ips
    ips = local_ips()
    print("=" * 60)
    print(" ARC Cluster MASTER chalu hoyeche")
    print(f" Ei PC-te browser:  http://localhost:{a.port}")
    for ip in ips:
        print(f" Worker-ra Master address hishebe dibe:  {ip}:{a.port}")
    print(" Bondho korte: Ctrl+C")
    print("=" * 60)
    if not a.no_browser:
        threading.Timer(1.0, lambda: webbrowser.open(f"http://localhost:{a.port}")).start()
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass
    stop.set()


if __name__ == "__main__":
    main()