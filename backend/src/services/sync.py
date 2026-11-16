"""线路货损汇总同步（源端）。

设计要点
- 组键 = 线路 + 温区。汇总只统计在该组内的班次及其样本：
  样本总数 total、缺失数 missing（暴露/超温/货损任一为空）、货损合计 loss_sum。
- 每次写样本 / 改班次时，在同一个数据库事务里更新受影响组（旧组与新组）的汇总，
  由发送线程从 outbox 推送。接收端断线只改变 outbox 状态，不会让样本保存失败。
- 幂等：每个组消息带单调递增的 version（该组聚合水位）。接收端只接受
  version 更大的消息，并按 (receipt_id, group_key) 去重，因此
  「已入账却丢回执」的重发不会重复累计；并发或乱序到达时旧版本被拒绝，
  最终收敛到源端同一份快照。
- 对账：接收端回执带 ledger_version 与 ledger_payload。源端只在接收端版本
  >= 本地版本时标记 reconciled（已对账），否则保留待确认 / 待发送。
  补发只重放当前快照，不会覆盖接收端已经入账的更新结果。
"""
import json
import logging
import os
import threading
import time
import uuid
from datetime import datetime, timezone

from sqlalchemy import update

from src.config.database import Session
from src.models.entities import (
    Sample,
    Shipment,
    SourceVersion,
    SyncCounter,
    SyncGroup,
    SyncOutbox,
)
from src.services.business import dump

_COUNTER_ID = 1


def now():
    return datetime.now(timezone.utc).isoformat(timespec='seconds')


def group_key(route, zone):
    return f'{route}|{zone}'


def ensure_counter(session):
    counter = session.get(SyncCounter, _COUNTER_ID)
    if not counter:
        counter = SyncCounter(id=_COUNTER_ID, value=0)
        session.add(counter)
        session.flush()
    return counter


def summarize(session, route, zone):
    """按线路 + 温区汇总样本。缺失 = 三项指标任一为空；货损合计只累加非空值。

    这里刻意不加 SELECT ... FOR UPDATE：写入方先取得全局水位行锁（见
    _allocate_seq），因此本组读取发生在所有更早写入提交之后；再锁样本行反而会
    与「已插入本组样本、正等水位锁」的并发事务互相等待。
    """
    shipments = session.query(Shipment).filter_by(route=route, zone=zone).all()
    ids = [r.id for r in shipments]
    samples = session.query(Sample).filter(Sample.shipment_id.in_(ids)).all() if ids else []
    missing = sum(1 for s in samples if s.exposure is None or s.excursions is None or s.loss is None)
    loss = round(sum(s.loss for s in samples if s.loss is not None), 4)
    return {
        'group_key': group_key(route, zone),
        'route': route,
        'zone': zone,
        'total': len(samples),
        'missing': missing,
        'loss_sum': loss,
        'shipments': len(shipments),
    }


def _allocate_seq(session):
    """全局写入水位。

    行锁保证并发写入串行化，也保证同一组摘要不会丢更新。版本取
    max(当前微秒时间, 上一版本+1)：既能在一个源库里严格单调，也能在源库
    重建后越过接收端已有的旧水位，避免对账永远卡在「接收端版本更新」。
    """
    counter = session.query(SyncCounter).filter_by(id=_COUNTER_ID).with_for_update().one_or_none()
    if counter is None:
        counter = ensure_counter(session)
    stamp = int(time.time() * 1_000_000)
    counter.value = max(stamp, int(counter.value or 0) + 1)
    session.flush()
    return int(counter.value)


def _touch_groups(session, pairs, action, entity, entity_id):
    """重算受影响组并入队；不提交，由调用方与业务写入一起提交。"""
    pairs = [p for p in pairs if p[0] is not None and p[1] is not None]
    # 去重且排序，避免两个事务以相反顺序加锁造成死锁
    pairs = sorted(set(pairs))
    if not pairs:
        return []
    seq = _allocate_seq(session)  # 行锁：此后读到的快照包含全部更早水位对应的写入
    session.add(SourceVersion(seq=seq, entity=entity, entity_id=entity_id,
                              action=action, created=now()))
    touched = []
    for route, zone in pairs:
        key = group_key(route, zone)
        payload = summarize(session, route, zone)
        group = session.query(SyncGroup).filter_by(route=route, zone=zone).one_or_none()
        if not group:
            group = SyncGroup(route=route, zone=zone, sent_seq=0)
            session.add(group)
            session.flush()
        group.sent_seq = seq
        group.sent_payload = payload
        group.updated = now()
        out = session.query(SyncOutbox).filter_by(key=key).one_or_none()
        if not out:
            out = SyncOutbox(key=key, route=route, zone=zone, version=seq,
                             payload=payload, state='pending', attempts=0,
                             last_error='', created=now(), updated=now(), ledger_version=0)
            session.add(out)
            session.flush()
        # 新事实推进本地版本；payload 永远是最新快照（绝对量，不是增量）
        out.version = seq
        out.payload = payload
        out.state = 'pending'
        out.last_error = ''
        out.last_attempt = None
        out.updated = now()
        touched.append(out)
    session.flush()
    return touched


def record_sample_change(session, action, sample, old_shipment=None):
    """样本新增 / 更正。old_shipment 为更正前的班次，用于同时校正旧组。"""
    new_ship = session.get(Shipment, sample.shipment_id) if sample.shipment_id else None
    pairs = []
    if old_shipment is not None:
        pairs.append((old_shipment.route, old_shipment.zone))
    if new_ship is not None:
        pairs.append((new_ship.route, new_ship.zone))
    return _touch_groups(session, pairs, action, 'sample', sample.id)


def record_shipment_change(session, action, shipment, old_route=None, old_zone=None):
    """班次改线路 / 温区：样本随班次迁移，旧组与新组都要校正。"""
    pairs = [(shipment.route, shipment.zone)]
    if old_route is not None:
        pairs.append((old_route, old_zone))
    return _touch_groups(session, pairs, action, 'shipment', shipment.id)


def backfill(session):
    """为尚未进入 outbox 的历史分组补一条待发送快照（重复执行不产生重复组）。"""
    ensure_counter(session)
    pairs = session.query(Shipment.route, Shipment.zone).distinct().all()
    existing = {r[0] for r in session.query(SyncOutbox.key).all()}
    missing = [(row[0], row[1]) for row in pairs
               if group_key(row[0], row[1]) not in existing]
    if missing:
        _touch_groups(session, missing, '回填分组', 'shipment', 0)
        session.commit()
        wake_dispatcher()
    return missing


def wake_dispatcher():
    _wake.set()


# ---- 发送与回执 ----

def _receiver_url():
    return os.environ.get('RECEIVER_URL', 'http://receiver:7000').rstrip('/') + '/api/ingest'


def _retry_seconds():
    try:
        return max(0.0, float(os.environ.get('SYNC_RETRY_SECONDS', '3')))
    except ValueError:
        return 3.0


def _claim(row_id, version):
    """原子地把一条 outbox 推进为 sending，并返回待发送内容。

    条件 UPDATE 让多个发送线程（或后端多副本）不会重复占用同一次发送；
    即使真的并发发出，接收端按 version 幂等，结果仍一致。
    receipt_id 由 (组键, 版本) 决定：同一版本的每次重试使用同一 receipt_id，
    接收端据此识别「已入账但回执丢失」的重试，不会重复累计。
    """
    stamp = now()
    with Session() as s:
        res = s.execute(
            update(SyncOutbox)
            .where(SyncOutbox.id == row_id,
                   SyncOutbox.version == version,
                   SyncOutbox.state.in_(['pending', 'sending', 'needs_resend']))
            .values(state='sending', attempts=SyncOutbox.attempts + 1,
                    last_attempt=stamp, updated=stamp)
        )
        if res.rowcount != 1:
            s.rollback()
            return None
        s.commit()
        row = s.get(SyncOutbox, row_id)
        return {
            'receipt_id': uuid.uuid5(uuid.NAMESPACE_URL, f'loss-sync:{row.key}:{int(row.version)}').hex,
            'group_key': row.key,
            'route': row.route,
            'zone': row.zone,
            'version': int(row.version),
            'payload': row.payload,
        }


def _apply_receipt(key, version, receipt):
    """处理接收端回执。重复回执不改变已对账结论，也不回退到旧状态。"""
    ledger_version = int(receipt.get('ledger_version') or 0)
    accepted = bool(receipt.get('accepted'))
    reason = receipt.get('reason') or ''
    ledger_payload = receipt.get('ledger_payload')
    with Session() as s:
        row = s.query(SyncOutbox).filter_by(key=key).with_for_update().one_or_none()
        if not row:
            return
        if int(version) < int(row.version):
            s.rollback()  # 回执属于已被新事实取代的旧版本，忽略并保留当前状态
            return
        row.receipt_version = ledger_version
        row.ledger_version = ledger_version
        if ledger_payload is not None:
            row.ledger_payload = ledger_payload
        local = int(row.version)
        if ledger_version == local:
            row.last_error = '' if accepted else (reason or '接收端拒绝该版本')
            row.state = 'reconciled'
        elif ledger_version > local:
            # 接收端持有比源端更新的内容：可能是源库重建。新写入会取更高版本自动收敛，
            # 这里显式标成待确认，界面展示差额并允许补发。
            if payload_fingerprint(ledger_payload) == payload_fingerprint(row.payload):
                row.state = 'reconciled'
                row.last_error = ''
            else:
                row.state = 'diverged'
                row.last_error = row.last_error or '接收端版本高于源端，请核对差额后补发或等待新写入'
        elif accepted:
            # 回执对应旧消息：本地还有更新内容待发送
            row.state = 'pending'
            row.last_error = ''
        else:
            row.last_error = reason or '接收端拒绝该版本'
            row.state = 'needs_resend'
        row.updated = now()
        s.commit()


def _apply_failure(key, version, reason):
    with Session() as s:
        row = s.query(SyncOutbox).filter_by(key=key).with_for_update().one_or_none()
        if not row:
            return
        if int(version) < int(row.version):
            s.rollback()  # 发送失败的是旧版本，新版本仍待发送
            return
        row.last_error = reason
        row.state = 'needs_resend'
        row.updated = now()
        s.commit()


def push_one(msg):
    """发送一条消息。断线、超时、非 2xx 都只记录状态，不向业务调用方抛错。"""
    body = json.dumps(msg).encode()
    try:
        import urllib.request
        req = urllib.request.Request(_receiver_url(), data=body,
                                     headers={'Content-Type': 'application/json'})
        timeout = float(os.environ.get('RECEIVER_TIMEOUT', '4'))
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            receipt = json.loads(resp.read().decode())
        _apply_receipt(msg['group_key'], msg['version'], receipt)
        return True
    except Exception as exc:  # 接收端不可达 / 丢回执 / 返回 5xx
        logging.info('sync send failed key=%s version=%s: %s',
                     msg['group_key'], msg['version'], exc)
        _apply_failure(msg['group_key'], msg['version'], f'发送失败：{exc}')
        return False


def _candidate():
    """选一条可发送记录：pending 立即发；sending / needs_resend 等退避后再试。"""
    delay = _retry_seconds()
    now_ts = time.time()
    with Session() as s:
        rows = s.query(SyncOutbox).filter(
            SyncOutbox.state.in_(['pending', 'sending', 'needs_resend'])
        ).order_by(SyncOutbox.id).all()
        for row in rows:
            if row.state == 'pending':
                return row.id, int(row.version)
            if row.last_attempt:
                try:
                    ts = datetime.fromisoformat(row.last_attempt).timestamp()
                except ValueError:
                    ts = 0
                if now_ts - ts < delay:
                    continue
            return row.id, int(row.version)
    return None


def dispatch_once():
    chosen = _candidate()
    if not chosen:
        return False
    msg = _claim(chosen[0], chosen[1])
    if not msg:
        return False
    push_one(msg)
    return True


def drain(limit=100, budget=None):
    """界面 / 演练辅助：把当前待发送记录各尝试一次。

    每条最多推一次，并有总时间预算，接收端离线时不会把一个请求拖成
    多次网络超时。失败的记录保留待确认状态，由后台线程按退避重试。
    """
    if budget is None:
        try:
            budget = float(os.environ.get('SYNC_DRAIN_SECONDS', '5'))
        except ValueError:
            budget = 5.0
    end = time.time() + budget
    with Session() as s:
        rows = [(r.id, int(r.version)) for r in s.query(SyncOutbox).filter(
            SyncOutbox.state.in_(['pending', 'sending', 'needs_resend'])
        ).order_by(SyncOutbox.id).all()]
    sent = 0
    for row_id, version in rows[:limit]:
        if time.time() >= end:
            break
        msg = _claim(row_id, version)
        if not msg:
            continue
        push_one(msg)
        sent += 1
    return sent


def resend(key):
    """界面补发：只把当前版本重新排队，不改 payload。"""
    with Session() as s:
        row = s.query(SyncOutbox).filter_by(key=key).one_or_none()
        if not row:
            return False
        row.state = 'pending'
        row.last_attempt = None
        row.updated = now()
        s.commit()
    wake_dispatcher()
    return True


def resume():
    """进程重启：把上次停在 sending 的记录退回待发送，并唤醒发送线程。"""
    with Session() as s:
        for row in s.query(SyncOutbox).filter_by(state='sending').all():
            row.state = 'pending'
            row.last_attempt = None
            row.updated = now()
        s.commit()
    wake_dispatcher()


_wake = threading.Event()
_stop = threading.Event()
_thread = None


def _loop():
    while not _stop.is_set():
        try:
            worked = False
            while dispatch_once():
                worked = True
            if not worked:
                _wake.wait(timeout=2)
                _wake.clear()
        except Exception:
            logging.exception('sync dispatcher')
            time.sleep(2)


def start_dispatcher():
    global _thread
    if _thread and _thread.is_alive():
        return
    resume()
    _stop.clear()
    _thread = threading.Thread(target=_loop, name='loss-sync', daemon=True)
    _thread.start()


def stop_dispatcher():
    _stop.set()
    _wake.set()


# ---- 页面视图 ----

def status_rows(session):
    rows = session.query(SyncOutbox).order_by(SyncOutbox.route, SyncOutbox.zone).all()
    out = []
    for r in rows:
        local = r.payload or {}
        remote = r.ledger_payload or {}
        diff = {
            'total': int(local.get('total', 0)) - int(remote.get('total', 0)),
            'missing': int(local.get('missing', 0)) - int(remote.get('missing', 0)),
            'loss_sum': round(float(local.get('loss_sum', 0)) - float(remote.get('loss_sum', 0)), 4),
        }
        out.append({
            **dump(r),
            'phase': phase_of(r.state),
            'diff': diff,
            'in_sync': (r.state == 'reconciled'
                        and diff['total'] == 0 and diff['missing'] == 0
                        and diff['loss_sum'] == 0),
        })
    summary = {
        'groups': len(out),
        'pending': sum(1 for r in out if r['phase'] == '待发送'),
        'confirming': sum(1 for r in out if r['phase'] == '待确认'),
        'reconciled': sum(1 for r in out if r['phase'] == '已对账'),
        'diverged': sum(1 for r in out if not r['in_sync']),
        'local_total': sum(int((r['payload'] or {}).get('total', 0)) for r in out),
        'remote_total': sum(int((r['ledger_payload'] or {}).get('total', 0)) for r in out),
        'local_missing': sum(int((r['payload'] or {}).get('missing', 0)) for r in out),
        'remote_missing': sum(int((r['ledger_payload'] or {}).get('missing', 0)) for r in out),
        'local_loss': round(sum(float((r['payload'] or {}).get('loss_sum', 0)) for r in out), 4),
        'remote_loss': round(sum(float((r['ledger_payload'] or {}).get('loss_sum', 0)) for r in out), 4),
    }
    return {'summary': summary, 'groups': out}


def phase_of(state):
    """页面三态：待发送 / 待确认 / 已对账。diverged 归入待确认，等待补发或新事实。"""
    if state == 'pending':
        return '待发送'
    if state in ('sending', 'needs_resend', 'diverged'):
        return '待确认'
    return '已对账'
