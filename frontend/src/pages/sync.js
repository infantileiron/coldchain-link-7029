import {state} from '../stores/session.js';
import {esc} from '../utils/html.js';
import {table,raw} from '../components/common/table.js';

const phaseClass = p => p === '已对账' ? 'ok' : p === '待确认' ? 'warn' : 'todo';
const badge = p => `<span class="tag ${phaseClass(p)}">${esc(p)}</span>`;
const fixed = n => Number(n || 0).toFixed(2);

export function render() {
  const groups = state.sync?.groups || [];
  const s = state.sync?.summary || {};
  const actionable = groups.filter(r => !r.in_sync);
  const disabled = state.user.role === 'viewer';
  return `<div class=split><section class=panel><h2>线路 / 温区货损汇总 · 两端对账</h2>
<p>样本保存与待发送变更在同一事务内生效；接收端断线只影响发送状态，不阻塞录入。恢复后自动续传，已入账但回执丢失时会带同一回执号重试，不重复累计。</p>
<div class=metrics><div><strong>${s.pending || 0}</strong>待发送</div><div><strong>${s.confirming || 0}</strong>待确认</div><div><strong>${s.reconciled || 0}</strong>已对账</div><div><strong>${s.diverged || 0}</strong>存在差额</div></div>
<p class=notice>两端差额：样本总数 ${(s.local_total || 0) - (s.remote_total || 0) >= 0 ? '+' : ''}${(s.local_total || 0) - (s.remote_total || 0)} / 缺失 ${(s.local_missing || 0) - (s.remote_missing || 0) >= 0 ? '+' : ''}${(s.local_missing || 0) - (s.remote_missing || 0)} / 货损合计 ${fixed((s.local_loss || 0) - (s.remote_loss || 0))}。补发只重放源端当前快照，不会覆盖接收端已经入账的新版本。</p>
${table(['组','样本总数','缺失数','货损合计','阶段','源版本','接收版本','差额','动作'], groups.map(r => [
  r.key, r.payload?.total ?? 0, r.payload?.missing ?? 0, fixed(r.payload?.loss_sum), raw(badge(r.phase)),
  r.version, r.ledger_version ?? 0,
  `总数 ${r.diff.total >= 0 ? '+' : ''}${r.diff.total} / 缺失 ${r.diff.missing >= 0 ? '+' : ''}${r.diff.missing} / 货损 ${fixed(r.diff.loss_sum)}`,
  r.in_sync ? '一致' : (disabled ? '只读' : raw(`<button data-resend="${esc(r.key)}">补发</button>`))
]))}
${actionable.length && !disabled ? `<p><button class=primary id=resend-all>补发全部有差额分组（${actionable.length}）</button></p>` : ''}
<h3>发送状态明细</h3>
${table(['组','阶段','尝试','源版本','接收版本','最近发送','说明'], groups.map(r => [
  r.key, r.phase, r.attempts, r.version, r.ledger_version ?? 0, r.last_attempt || '—', r.last_error || '—'
]))}
</section><aside class=side><h2>接收端</h2>
<p>独立接收端只保存每个线路 / 温区最新入账的快照。并发与乱序按 (组, 版本) 与回执号去重，旧版本晚到会被拒绝。</p>
<p class=eyebrow>字段口径</p>
<p>样本总数 = 组内班次的样本条数<br>缺失数 = 暴露 / 超温 / 货损任一为空<br>货损合计 = 非空货损率之和</p>
${disabled ? '' : '<p><button id=refresh-sync>立即尝试发送并刷新</button></p>'}
<p class=eyebrow>演示</p>
<p>可本地启动接收端后关闭再打开，观察待发送 / 待确认 / 已对账的变化；补发按钮不会改动接收端已确认的数字。</p>
</aside></div>`;
}
