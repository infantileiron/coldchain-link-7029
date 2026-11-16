"""线路货损汇总 · 独立接收端。

这是一个可以单独启动、单独持久化的对账端，只接收源端推来的「组快照」：
组键 = 线路|温区，消息带单调递增 version 和绝对量 payload。

幂等与顺序
- 以 (group_key, version) 作为账本水位，只接受更大的版本；重复或更旧的版本
  返回 accepted=false 与当前账本版本，不覆盖已入账的更新结果。
- receipt_id 落库去重：同一 receipt_id 再次到达直接回执同一结果，
  因此「已入账但回执丢失」后源端重试不会重复累计。
- 写入在一个 SQLite 事务内完成（BEGIN IMMEDIATE），并发到达按到达顺序串行，
  先入账的低版本不会被后到的旧版本改写。
- 账本在 SQLite 文件中，进程/容器重启后仍在。
"""
import json
import os
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, timezone

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

DB_PATH = os.environ.get('RECEIVER_DB', '/data/receiver.sqlite')
_lock = threading.Lock()

SCHEMA = """
CREATE TABLE IF NOT EXISTS ledger (
  group_key TEXT PRIMARY KEY,
  route TEXT NOT NULL,
  zone TEXT NOT NULL,
  version INTEGER NOT NULL,
  payload TEXT NOT NULL,
  updated TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS receipts (
  receipt_id TEXT PRIMARY KEY,
  group_key TEXT NOT NULL,
  version INTEGER NOT NULL,
  accepted INTEGER NOT NULL,
  reason TEXT,
  ledger_version INTEGER NOT NULL,
  created TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS ingest_log (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  receipt_id TEXT NOT NULL,
  group_key TEXT NOT NULL,
  version INTEGER NOT NULL,
  accepted INTEGER NOT NULL,
  reason TEXT,
  created TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS chaos (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  drop_receipts INTEGER NOT NULL DEFAULT 0
);
"""


def now():
    return datetime.now(timezone.utc).isoformat(timespec='seconds')


@contextmanager
def db():
    conn = sqlite3.connect(DB_PATH, timeout=30, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA journal_mode=WAL')
    conn.execute('PRAGMA busy_timeout=30000')
    try:
        yield conn
    finally:
        conn.close()


def init():
    os.makedirs(os.path.dirname(DB_PATH) or '.', exist_ok=True)
    with db() as conn:
        conn.executescript(SCHEMA)
        conn.execute('INSERT OR IGNORE INTO chaos(id) VALUES (1)')


app = FastAPI(title='货损汇总接收端')


@app.on_event('startup')
def startup():
    init()


@app.get('/health')
def health():
    with db() as conn:
        conn.execute('SELECT 1')
    return {'status': 'ok', 'db': DB_PATH}


def _receipt(group_key, accepted, reason, ledger_version, ledger_payload):
    return {
        'accepted': accepted,
        'reason': reason,
        'group_key': group_key,
        'ledger_version': int(ledger_version),
        'ledger_payload': ledger_payload,
    }


def _log(conn, receipt_id, group_key, version, accepted, reason, ledger_version):
    stamp = now()
    conn.execute(
        'INSERT OR REPLACE INTO receipts(receipt_id,group_key,version,accepted,reason,ledger_version,created)'
        ' VALUES(?,?,?,?,?,?,?)',
        (receipt_id, group_key, version, 1 if accepted else 0, reason, ledger_version, stamp))
    conn.execute(
        'INSERT INTO ingest_log(receipt_id,group_key,version,accepted,reason,created) VALUES(?,?,?,?,?,?)',
        (receipt_id, group_key, version, 1 if accepted else 0, reason, stamp))


@app.post('/api/ingest')
async def ingest(request: Request):
    try:
        msg = await request.json()
        group_key = str(msg['group_key'])
        version = int(msg['version'])
        receipt_id = str(msg.get('receipt_id') or '')
        payload = msg['payload']
        route = str(msg.get('route') or payload.get('route') or '')
        zone = str(msg.get('zone') or payload.get('zone') or '')
    except Exception:
        return JSONResponse(status_code=422, content={'detail': '消息格式不正确'})

    with _lock, db() as conn:
        conn.execute('BEGIN IMMEDIATE')
        try:
            chaos = conn.execute('SELECT drop_receipts FROM chaos WHERE id=1').fetchone()
            drop = bool(chaos['drop_receipts']) if chaos else False
            row = conn.execute('SELECT * FROM ledger WHERE group_key=?', (group_key,)).fetchone()
            current = int(row['version']) if row else 0
            current_payload = json.loads(row['payload']) if row else None

            prior = (conn.execute('SELECT * FROM receipts WHERE receipt_id=?', (receipt_id,)).fetchone()
                     if receipt_id else None)
            if prior:
                # 重试同一条消息：账本不变，回执同一结果。即使演练丢回执，
                # 重试也只会确认已入账结果，不会重复累计。
                conn.commit()
                body = _receipt(group_key, bool(prior['accepted']), prior['reason'],
                                current, current_payload)
            elif row and version <= current:
                _log(conn, receipt_id, group_key, version, False, 'stale_or_duplicate', current)
                conn.commit()
                body = _receipt(group_key, False, 'stale_or_duplicate', current, current_payload)
            else:
                stamp = now()
                conn.execute(
                    'INSERT INTO ledger(group_key,route,zone,version,payload,updated)'
                    ' VALUES(?,?,?,?,?,?)'
                    ' ON CONFLICT(group_key) DO UPDATE SET route=excluded.route, zone=excluded.zone,'
                    ' version=excluded.version, payload=excluded.payload, updated=excluded.updated',
                    (group_key, route, zone, version, json.dumps(payload, ensure_ascii=False), stamp))
                _log(conn, receipt_id, group_key, version, True, '', version)
                conn.commit()
                body = _receipt(group_key, True, '', version, payload)
        except Exception:
            conn.rollback()
            raise

    if drop:
        # 已入账，但回执在返回路上丢了（502）。源端会带着同一 receipt_id 重试。
        return JSONResponse(status_code=502, content={'detail': '回执丢失（演练）'})
    return body


@app.get('/api/ledger')
def ledger():
    with db() as conn:
        rows = conn.execute('SELECT * FROM ledger ORDER BY route,zone').fetchall()
    return [{
        'group_key': r['group_key'],
        'route': r['route'],
        'zone': r['zone'],
        'version': r['version'],
        'payload': json.loads(r['payload']),
        'updated': r['updated'],
    } for r in rows]


@app.get('/api/receipts')
def receipts(limit: int = 100):
    with db() as conn:
        rows = conn.execute('SELECT * FROM receipts ORDER BY rowid DESC LIMIT ?', (limit,)).fetchall()
    return [dict(r) for r in rows]


@app.get('/api/log')
def ingest_log(limit: int = 200):
    with db() as conn:
        rows = conn.execute('SELECT * FROM ingest_log ORDER BY id DESC LIMIT ?', (limit,)).fetchall()
    return [dict(r) for r in rows]


@app.post('/api/chaos/drop-receipts')
async def chaos_drop(request: Request):
    """演练开关：照常入账，再丢回执。请求体 {"enabled": true}。"""
    body = {}
    try:
        body = await request.json() or {}
    except Exception:
        pass
    enabled = 1 if body.get('enabled') else 0
    with _lock, db() as conn:
        conn.execute('UPDATE chaos SET drop_receipts=? WHERE id=1', (enabled,))
    return {'drop_receipts': bool(enabled)}


@app.post('/api/chaos/reset')
def chaos_reset():
    """清空账本，用于重新演练。"""
    with _lock, db() as conn:
        conn.execute('BEGIN IMMEDIATE')
        conn.execute('DELETE FROM ledger')
        conn.execute('DELETE FROM receipts')
        conn.execute('DELETE FROM ingest_log')
        conn.execute('UPDATE chaos SET drop_receipts=0')
        conn.commit()
    return {'ok': True}
