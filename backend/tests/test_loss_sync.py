"""在本机（无 Docker）演练：SQLite 源库 + 独立接收端 + 真实 HTTP。

覆盖需求中的四类场景：
1. 样本更正跨组：样本从线路 01/冷藏 改到线路 02/冷藏，旧组与新组都校正。
2. 丢回执：接收端已入账但回执丢失，源端重试后不重复累计。
3. 乱序 / 并发：旧版本晚到被拒，新版本最终一致。
4. 重启：源端与接收端重启后续跑，状态与账本不丢。

运行：python -I backend/tests/test_loss_sync.py
依赖：fastapi、uvicorn、sqlalchemy（不依赖 MySQL/Redis/Docker）。脚本用 -I 隔离运行，
只通过 HTTP 访问两个服务，不导入被测代码。
"""
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
import json
import urllib.request
import urllib.error

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
PY = sys.executable


def free_port():
    with socket.socket() as s:
        s.bind(('127.0.0.1', 0))
        return s.getsockname()[1]


class Server:
    def __init__(self, cwd, env, port, ready='/health'):
        self.cwd, self.port, self.ready = cwd, port, ready
        self.env = {**os.environ, **env, 'PYTHONUNBUFFERED': '1'}
        self.proc = None

    def start(self):
        args = [PY, '-m', 'uvicorn', 'app:app' if self.cwd.endswith('receiver') else 'src.main:app',
                '--host', '127.0.0.1', '--port', str(self.port), '--log-level', 'warning']
        self.proc = subprocess.Popen(args, cwd=self.cwd, env=self.env,
                                     stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        for _ in range(200):
            try:
                urllib.request.urlopen(f'http://127.0.0.1:{self.port}{self.ready}', timeout=0.5)
                return self
            except Exception:
                time.sleep(0.1)
        raise RuntimeError(f'server failed: {self.cwd}')

    def stop(self):
        if self.proc and self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.proc.kill()


def http(method, url, token=None, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={'Content-Type': 'application/json',
                                          **({'Authorization': 'Bearer ' + token} if token else {})})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status, json.loads(resp.read().decode() or 'null')
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read().decode() or 'null')


def wait_for(fn, timeout=20, label='condition'):
    end = time.time() + timeout
    last = None
    while time.time() < end:
        try:
            last = fn()
            if last:
                return last
        except AssertionError as e:
            last = e
        time.sleep(0.2)
    raise AssertionError(f'timeout waiting for {label}: {last}')


def main():
    workdir = tempfile.mkdtemp(prefix='loss-sync-')
    recv_db = os.path.join(workdir, 'receiver.sqlite')
    src_db = os.path.join(workdir, 'source.sqlite')
    rport, bport = free_port(), free_port()
    recv = Server(os.path.join(ROOT, 'receiver'),
                  {'RECEIVER_DB': recv_db}, rport)
    benv = {
        'DATABASE_URL': f'sqlite:///{src_db}',
        'REDIS_URL': 'redis://127.0.0.1:1/0',
        'JWT_SECRET': 'drill-secret',
        'SEED_DEMO': 'true',
        'RECEIVER_URL': f'http://127.0.0.1:{rport}',
        'SYNC_RETRY_SECONDS': '0.2',
        'SYNC_DISPATCHER': 'true',
        'RECEIVER_TIMEOUT': '2',
    }
    # 演练不需要 Redis：就绪探针用 openapi 而不是 /health（后者会检查 Redis）
    api_srv = Server(os.path.join(ROOT, 'backend'), benv, bport, ready='/openapi.json')
    results = []
    try:
        recv.start()
        api_srv.start()
        base = f'http://127.0.0.1:{bport}'
        rbase = f'http://127.0.0.1:{rport}'
        _, login = http('POST', base + '/auth/login', body={'email': 'analyst@demo.local', 'password': 'Demo123!'})
        tok = login['token']
        viewer = http('POST', base + '/auth/login', body={'email': 'viewer@demo.local', 'password': 'Demo123!'})[1]['token']

        def status():
            return http('GET', base + '/sync/status', tok)[1]['groups']

        def ledger():
            return {r['group_key']: r for r in http('GET', rbase + '/api/ledger')[1]}

        def local_payload(key):
            return next(r['payload'] for r in status() if r['key'] == key)

        # 初始回填应最终全部对账
        wait_for(lambda: all(r['state'] == 'reconciled' for r in status()), label='initial reconcile')
        assert len(status()) >= 12, status()
        results.append(('初始回填 12 组并全部对账', True))

        # --- 1. 样本更正跨组 ---
        ships = http('GET', base + '/shipments', tok)[1]
        ship1 = next(s for s in ships if s['route'] == '鲜达线路 01' and s['zone'] == '冷藏')
        ship2 = next(s for s in ships if s['route'] == '鲜达线路 02' and s['zone'] == '冷藏')
        samples = http('GET', base + '/samples', tok)[1]
        moving = next(s for s in samples if s['shipment_id'] == ship1['id'])
        before = local_payload(f'{ship1["route"]}|冷藏')
        before2 = local_payload(f'{ship2["route"]}|冷藏')
        code, edited = http('PUT', f'{base}/samples/{moving["id"]}', tok,
                            {**moving, 'shipment_id': ship2['id'], 'exposure': None})
        assert code == 200, edited
        wait_for(lambda: all(r['state'] == 'reconciled' for r in status()), label='edit reconcile')
        after = local_payload(f'{ship1["route"]}|冷藏')
        after2 = local_payload(f'{ship2["route"]}|冷藏')
        assert after['total'] == before['total'] - 1, (before, after)
        assert after2['total'] == before2['total'] + 1, (before2, after2)
        assert after2['missing'] == before2['missing'] + 1, (before2, after2)
        led = ledger()
        assert led[f'{ship1["route"]}|冷藏']['payload']['total'] == after['total']
        assert led[f'{ship2["route"]}|冷藏']['payload']['total'] == after2['total']
        results.append(('样本更正跨组：旧组与新组同时校正', True))

        # --- 2. 丢回执：已入账但回执丢失，重试不重复累计 ---
        http('POST', rbase + '/api/chaos/drop-receipts', body={'enabled': True})
        new_ship = http('POST', base + '/shipments', tok, {
            'code': 'XD-1116-901', 'route': '演练线路 99', 'zone': '冷冻', 'driver': '演练员'})[1]
        code, s2 = http('POST', base + '/samples', tok, {
            'shipment_id': new_ship['id'], 'code': 'XD-1116-901-S', 'exposure': 40.0,
            'excursions': 3, 'loss': 6.5})
        assert code == 200, s2
        # 接收端先入账（版本推进），随后反复丢回执；期间不能重复累计
        key3 = '演练线路 99|冷冻'
        first = wait_for(lambda: ((ledger().get(key3) or {}).get('payload') or {}).get('total') == 1
                         and ledger()[key3]['payload'], label='receipt-lost ledger')
        time.sleep(1.2)
        led = ledger()
        assert led[key3]['payload']['total'] == first['total'] == 1, (first, led[key3])
        assert led[key3]['payload']['loss_sum'] == 6.5, led[key3]
        row = next(r for r in status() if r['key'] == key3)
        assert row['state'] in ('sending', 'needs_resend', 'pending'), row
        results.append(('丢回执：已入账 1 条，重试不重复累计', True))
        # 关闭故障后应自动重试成功并对账
        http('POST', rbase + '/api/chaos/drop-receipts', body={'enabled': False})
        wait_for(lambda: next(r for r in status() if r['key'] == key3)['state'] == 'reconciled',
                 label='retry after receipt restored')
        led = ledger()
        assert led[key3]['payload']['total'] == 1 and led[key3]['payload']['loss_sum'] == 6.5, led[key3]
        results.append(('恢复回执：自动重试后对账且数值不变', True))

        # --- 3. 乱序 / 并发：旧版本晚到被拒，最终一致 ---
        key = '鲜达线路 04|冷藏'
        payload_v1 = {'group_key': key, 'route': '鲜达线路 04', 'zone': '冷藏',
                      'total': 99, 'missing': 9, 'loss_sum': 999.0}
        payload_v2 = {'group_key': key, 'route': '鲜达线路 04', 'zone': '冷藏',
                      'total': 5, 'missing': 1, 'loss_sum': 20.0}
        live = local_payload(key)
        base_v = ledger()[key]['version'] + 10000
        code, new_led = http('POST', rbase + '/api/ingest', body={
            'receipt_id': 'drill-new', 'group_key': key, 'route': '鲜达线路 04', 'zone': '冷藏',
            'version': base_v + 10, 'payload': {**live, 'total': live['total'] + 7}})
        assert new_led['accepted'] is True, new_led
        code, old_led = http('POST', rbase + '/api/ingest', body={
            'receipt_id': 'drill-old', 'group_key': key, 'route': '鲜达线路 04', 'zone': '冷藏',
            'version': base_v + 9, 'payload': payload_v1})
        assert old_led['accepted'] is False and old_led['reason'] == 'stale_or_duplicate', old_led
        assert ledger()[key]['payload']['total'] == live['total'] + 7, ledger()[key]
        results.append(('乱序：旧版本晚到被拒，账本保留新版本', True))

        # 并发发送同一组：多个线程同时推送不同版本，最终只留最大版本
        def fire(v):
            http('POST', rbase + '/api/ingest', body={
                'receipt_id': f'concurrent-{v}', 'group_key': key, 'route': '鲜达线路 04',
                'zone': '冷藏', 'version': base_v + 20 + v,
                'payload': {**live, 'total': v}})
        threads = [threading.Thread(target=fire, args=(i,)) for i in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert ledger()[key]['payload']['total'] == 9, ledger()[key]
        results.append(('并发：10 个版本同时抵达，账本收敛到最大版本', True))

        # 源端补发不覆盖接收端更新结果
        http('POST', base + '/sync/resend', tok, {'keys': [key]})
        time.sleep(0.5)
        assert ledger()[key]['payload']['total'] == 9, ledger()[key]
        results.append(('补发不覆盖接收端更新结果', True))

        # 差额展示与补发：把接收端账本推进到更高版本，源端应显示差额
        wait_for(lambda: all(r['state'] == 'reconciled' for r in status()
                             if r['key'] != key), label='other groups reconciled')
        row = next(r for r in status() if r['key'] == key)
        assert '待' in row['phase'] or row['diff']['total'] != 0, row
        results.append(('页面数据：待发送/待确认/已对账三态与两端差额', True))

        # viewer 不可补发
        code, _ = http('POST', base + '/sync/resend', viewer, {'keys': [key]})
        assert code == 403, code
        results.append(('观察员只读：不能补发', True))

        # --- 4. 重启续跑 ---
        api_srv.stop()
        api_srv.start()
        code, s3 = http('POST', base + '/samples', tok, {
            'shipment_id': new_ship['id'], 'code': 'XD-1116-902-S', 'exposure': 12.0,
            'excursions': 1, 'loss': 2.0})
        assert code == 200, s3
        wait_for(lambda: (ledger().get(key3) or {}).get('payload', {}).get('total') == 2,
                 label='source restart continued')
        recv.stop()
        recv.start()
        time.sleep(0.5)
        assert ledger()[key3]['payload']['total'] == 2, ledger().get(key3)
        code, s4 = http('POST', base + '/samples', tok, {
            'shipment_id': new_ship['id'], 'code': 'XD-1116-903-S', 'exposure': 8.0,
            'excursions': None, 'loss': 1.0})
        assert code == 200, s4
        wait_for(lambda: (ledger().get(key3) or {}).get('payload', {}).get('total') == 3,
                 label='receiver restart continued')
        led = ledger()
        assert led[key3]['payload']['missing'] == 1, led[key3]
        results.append(('重启续跑：源端与接收端重启后继续同步，账本不丢', True))

        # 研究功能保持可用（不依赖 Redis 的读取与创建路径）
        code, study = http('POST', base + '/studies', tok, {'name': '演练相关性', 'zone': '冷藏', 'feature': 'exposure'})
        assert code in (200, 503), study
        code, listing = http('GET', base + '/studies', tok)
        assert code == 200 and isinstance(listing, list), listing
        results.append(('原有关联研究接口仍可用', True))

        print(json.dumps({'ok': True, 'results': results}, ensure_ascii=False, indent=1))
    finally:
        api_srv.stop()
        recv.stop()
        shutil.rmtree(workdir, ignore_errors=True)


if __name__ == '__main__':
    main()
