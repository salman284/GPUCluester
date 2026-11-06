"""
worker.py  -  WORKER PC-te chalate hobe (prottek Worker PC-te).
 * Browser-e http://localhost:8001 khule Master address + Worker ID + Password diye Register korun
 * Register-e ei PC-r IP, CPU, RAM, GPU Master-ke pathano hoy
 * Master "Approve" korle task neya shuru korbe; nijer GPU-te (na pele CPU-te) kaj kore result Master-ke pathay
 * Alada database: worker.db
"""
import argparse
import json
import os
import threading
import time
import webbrowser
import urllib.error
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from common import DB, Engine, post_json, run_task, system_specs

db = None
state = {"status": "not registered", "message": "", "engine": "-", "engine_error": None,
         "current": None, "connected": False, "done": 0}
state_lock = threading.Lock()
wake = threading.Event()


def init_db(path):
    global db
    db = DB(path)
    db.run("CREATE TABLE IF NOT EXISTS config(k TEXT PRIMARY KEY, v TEXT)")
    db.run("""CREATE TABLE IF NOT EXISTS log(id INTEGER PRIMARY KEY AUTOINCREMENT, ts REAL,
              task_no INTEGER, value REAL, engine TEXT, ms REAL, result TEXT)""")
    state["done"] = db.one("SELECT COUNT(*) c FROM log WHERE result='accepted'")["c"]


def cfg(k, default=None):
    r = db.one("SELECT v FROM config WHERE k=?", (k,))
    return r["v"] if r else default


def set_cfg(k, v):
    db.run("INSERT OR REPLACE INTO config(k,v) VALUES(?,?)", (k, v))


def master_url():
    return f"http://{cfg('master')}"


def register(master, node_id, password):
    """Master-e register/login kore. Return (ok, message)."""
    if ":" not in master:
        master += ":8000"
    try:
        r = post_json(f"http://{master}/api/worker/register",
                      {"node_id": node_id, "password": password, "specs": system_specs()}, timeout=15)
    except urllib.error.HTTPError as e:
        try:
            return False, json.loads(e.read().decode()).get("error", str(e))
        except Exception:
            return False, str(e)
    except Exception as e:
        return False, f"Master-er shathe connect hoy ni ({e})"
    if not r.get("ok"):
        return False, r.get("error", "error")
    set_cfg("master", master)
    set_cfg("node_id", node_id)
    set_cfg("password", password)
    with state_lock:
        state["status"] = r.get("status", "pending")
    wake.set()
    return True, r.get("note", "ok")


# ---------------------------------------------------------------- background worker loop
def worker_loop(prefer_gpu):
    engine = Engine(prefer_gpu)
    with state_lock:
        state["engine"] = engine.label
        state["engine_error"] = engine.error
    print(f"[worker] engine: {engine.label}" + (f"  ({engine.error})" if engine.error else ""))
    pending_result = None   # compute hoyeche kintu Master ack dey ni -- abar pathabo

    while True:
        if not cfg("node_id"):
            wake.wait(2)
            wake.clear()
            continue
        auth = {"node_id": cfg("node_id"), "password": cfg("password")}
        try:
            if pending_result:
                r = post_json(master_url() + "/api/worker/result", {**auth, **pending_result})
                if r.get("ok"):
                    db.run("INSERT INTO log(ts,task_no,value,engine,ms,result) VALUES(?,?,?,?,?,?)",
                           (time.time(), pending_result["task_no"], pending_result["value"],
                            pending_result["engine"], pending_result["ms"], r.get("result")))
                    if r.get("result") == "accepted":
                        with state_lock:
                            state["done"] += 1
                    pending_result = None
                    with state_lock:
                        state["current"] = None
                continue

            r = post_json(master_url() + "/api/worker/task", {**auth, "engine": engine.label})
            with state_lock:
                state["connected"] = True
                state["status"] = r.get("status", "?")
            t = r.get("task")
            if not t:
                time.sleep(2)
                continue
            with state_lock:
                state["current"] = f"TASK-{t['task_no']:03d}"
            t0 = time.time()
            out = run_task(engine, {"task_no": t["task_no"], "n": t["n"]})
            pending_result = {"task_id": t["task_id"], "task_no": t["task_no"], "value": out["value"],
                              "engine": out["engine"], "ms": (time.time() - t0) * 1000}
        except urllib.error.HTTPError as e:
            with state_lock:
                state["connected"] = False
                state["message"] = "Master login reject korlo (ID/password?)" if e.code == 401 else str(e)
            time.sleep(5)
        except Exception as e:
            with state_lock:
                state["connected"] = False
                state["message"] = f"Master-er shathe connection nei: {e}"
            time.sleep(5)


# ---------------------------------------------------------------- local UI
class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def send_json(self, obj, code=200):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path == "/api/state":
            with state_lock:
                s = dict(state)
            s["master"] = cfg("master")
            s["node_id"] = cfg("node_id")
            s["specs"] = system_specs_cached()
            s["log"] = db.all("SELECT ts,task_no,value,engine,ms,result FROM log ORDER BY id DESC LIMIT 15")
            return self.send_json(s)
        body = PAGE.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        n = int(self.headers.get("Content-Length") or 0)
        d = json.loads(self.rfile.read(n).decode() or "{}")
        if self.path == "/api/register":
            ok, msg = register(str(d.get("master", "")).strip(), str(d.get("node_id", "")).strip(),
                               str(d.get("password", "")))
            return self.send_json({"ok": ok, "message": msg})
        if self.path == "/api/forget":
            for k in ("node_id", "password"):
                db.run("DELETE FROM config WHERE k=?", (k,))
            with state_lock:
                state["status"] = "not registered"
            return self.send_json({"ok": True})
        self.send_json({"error": "not found"}, 404)


_specs = None


def system_specs_cached():
    global _specs
    if _specs is None:
        _specs = system_specs()
    return _specs


PAGE = r"""<!doctype html><html lang="bn"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>ARC Cluster - Worker</title><style>
:root{--bg:#f5f6f8;--card:#fff;--tx:#1b1f24;--mut:#68707a;--ln:#dde1e6;--ac:#1f6feb;--ok:#1a7f37;--bad:#cf222e;--warn:#9a6700}
@media(prefers-color-scheme:dark){:root{--bg:#0f1318;--card:#171c23;--tx:#e6e9ed;--mut:#8b949e;--ln:#2b323b;--ac:#4c9aff;--ok:#3fb950;--bad:#f85149;--warn:#d29922}}
*{box-sizing:border-box}body{margin:0;font:14px/1.45 system-ui,Segoe UI,sans-serif;background:var(--bg);color:var(--tx)}
main{max-width:820px;margin:0 auto;padding:16px}h1{font-size:18px}
.card{background:var(--card);border:1px solid var(--ln);border-radius:10px;padding:14px 16px;margin-bottom:14px}
.card h2{font-size:13px;margin:0 0 10px;text-transform:uppercase;letter-spacing:.04em;color:var(--mut)}
input{width:100%;padding:8px 10px;margin:4px 0 10px;border:1px solid var(--ln);border-radius:6px;background:var(--bg);color:var(--tx);font:inherit}
button{padding:8px 14px;border:0;border-radius:6px;background:var(--ac);color:#fff;font:inherit;cursor:pointer}button.s{background:transparent;color:var(--tx);border:1px solid var(--ln)}
table{width:100%;border-collapse:collapse}td,th{text-align:left;padding:5px 6px;border-bottom:1px solid var(--ln)}th{color:var(--mut);font-size:12px}
.mut{color:var(--mut)}.ok{color:var(--ok)}.bad{color:var(--bad)}.warn{color:var(--warn)}label{font-size:12px;color:var(--mut)}
</style></head><body><main><h1>ARC Cluster — WORKER</h1><div id="app"></div></main><script>
const $=s=>document.querySelector(s),esc=s=>String(s??'').replace(/[&<>"]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));
const api=async(p,b)=>(await fetch(p,b===undefined?{}:{method:'POST',body:JSON.stringify(b)})).json();
let formShown=null;
async function tick(){
 const s=await api('/api/state');const reg=!!s.node_id;
 if(!reg){ if(formShown!==false){formShown=false;
  $('#app').innerHTML=`<div class="card"><h2>Register (Master-e ei PC-ke jukto korun)</h2>
  <label>Master address (Master PC-r IP:port, jemon 172.10.22.15:8000)</label><input id="m" placeholder="172.10.22.15:8000">
  <label>Worker ID (prottek PC-r alada, jemon WORKER-1)</label><input id="i" placeholder="WORKER-1">
  <label>Password (Worker-er jonno nijer thik korun)</label><input id="p" type="password">
  <div id="e" class="bad"></div><button id="go">Register / Login</button>
  <p class="mut">Register korle Master-e ei PC-r IP, CPU, RAM, GPU pathano hobe. Master "Approve" korle kaj shuru hobe.</p></div>
  <div class="card"><h2>Ei PC-r specs</h2><div id="sp"></div></div>`;
  $('#go').onclick=async()=>{const r=await api('/api/register',{master:$('#m').value,node_id:$('#i').value,password:$('#p').value});$('#e').textContent=r.ok?'':r.message;if(r.ok){formShown=null}}}
  const sp=s.specs;$('#sp')&&($('#sp').innerHTML=specTable(sp));return}
 formShown=true;
 const st=s.status,cls=st==='approved'?'ok':(st==='pending'?'warn':'bad');
 const msg={approved:'Approved — Master task dile kaj korbe',pending:'Pending — Master dashboard theke Approve korar opekkha',disabled:'Master ei PC-ke Disable kore rekheche'}[st]||st;
 let h=`<div class="card"><h2>Status</h2><p><b>${esc(s.node_id)}</b> → ${esc(s.master)} &nbsp; <span class="${s.connected?'ok':'bad'}">● ${s.connected?'connected':'not connected'}</span></p>
 <p class="${cls}"><b>${msg}</b></p>${s.message&&!s.connected?`<p class="bad">${esc(s.message)}</p>`:''}
 <p>Engine: <b>${esc(s.engine)}</b>${s.engine_error?`<br><span class="warn">${esc(s.engine_error)}</span>`:''}</p>
 <p>Ekhon: <b>${esc(s.current||'idle')}</b> &nbsp; Mot accepted task: <b>${s.done}</b></p>
 <button class="s" id="fg">Disconnect / ID bodlano</button></div>
 <div class="card"><h2>Ei PC-r specs</h2>${specTable(s.specs)}</div>
 <div class="card"><h2>Shesh task gulo</h2><table><tr><th>Task</th><th>Result</th><th>ms</th><th>Master reply</th></tr>
 ${s.log.map(l=>`<tr><td>TASK-${String(l.task_no).padStart(3,'0')}</td><td>${(+l.value).toFixed(2)}</td><td>${Math.round(l.ms)}</td><td>${esc(l.result)}</td></tr>`).join('')}</table></div>`;
 $('#app').innerHTML=h;$('#fg').onclick=async()=>{if(confirm('Disconnect korben?')){await api('/api/forget',{});formShown=null}};
}
function specTable(p){return `<table>${[['Hostname',p.hostname],['IP',(p.ips||[]).join(', ')],['OS',p.os],['CPU',p.cpu+' ('+p.cpu_cores+' cores)'],['RAM',p.ram_gb+' GB'],['GPU',p.gpu],['Python',p.python]].map(r=>`<tr><td class="mut">${r[0]}</td><td>${esc(r[1])}</td></tr>`).join('')}</table>`}
tick();setInterval(tick,2000);
</script></body></html>"""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ui-port", type=int, default=8001)
    ap.add_argument("--data-dir", default=".")
    ap.add_argument("--no-browser", action="store_true")
    ap.add_argument("--no-gpu", action="store_true")
    # CLI theke register (testing / headless):
    ap.add_argument("--master")
    ap.add_argument("--id")
    ap.add_argument("--password")
    a = ap.parse_args()

    os.makedirs(a.data_dir, exist_ok=True)
    init_db(os.path.join(a.data_dir, "worker.db"))
    if a.master and a.id and a.password:
        ok, msg = register(a.master, a.id, a.password)
        print("[register]", ok, msg)

    threading.Thread(target=worker_loop, args=(not a.no_gpu,), daemon=True).start()
    srv = ThreadingHTTPServer(("127.0.0.1", a.ui_port), Handler)
    srv.daemon_threads = True
    print("=" * 60)
    print(" ARC Cluster WORKER chalu hoyeche")
    print(f" Browser:  http://localhost:{a.ui_port}")
    print(" Bondho korte: Ctrl+C")
    print("=" * 60)
    if not a.no_browser:
        threading.Timer(1.0, lambda: webbrowser.open(f"http://localhost:{a.ui_port}")).start()
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
