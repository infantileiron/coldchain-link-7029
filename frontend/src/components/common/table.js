import {esc} from '../../utils/html.js';
// 单元格既可以是普通值，也可以是 {html} 片段（仅用于本模块自造的受控标记，不接用户输入原文）
export const raw = html => ({html});
export function table(headers,rows){return `<div class=scroll><table><thead><tr>${headers.map(h=>`<th>${esc(h)}</th>`).join('')}</tr></thead><tbody>${rows.map(r=>`<tr>${r.map(v=>`<td>${v&&typeof v==='object'&&'html' in v?v.html:esc(v)}</td>`).join('')}</tr>`).join('')||'<tr><td>暂无记录。请在右侧录入首条数据。</td></tr>'}</tbody></table></div>`}
