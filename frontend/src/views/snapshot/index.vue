<template>
  <section class="page" data-module="snapshot">
    <header class="page-head">
      <div><h2>数据快照</h2><p class="page-desc">按恢复检查点推进快照分片完成状态：暂停后恢复只重扫未完成分片，已完成分片进入复用清单，时间线按唯一完成顺序展示。</p></div>
      <div class="page-actions">
        <button class="btn primary" type="button" data-testid="open-create" @click="openCreate">登记快照记录</button>
        <button class="btn" type="button" data-testid="export" @click="exportRows">导出作业清单</button>
      </div>
    </header>
    <div class="filter-bar">
      <label><span>开始日期</span><input type="date" v-model="dateFrom" /></label>
      <label><span>结束日期</span><input type="date" v-model="dateTo" /></label>
      <button class="btn" type="button" @click="reload">查询</button>
    </div>
    <p v-if="errorMessage" class="error-text" data-testid="error-message">{{ errorMessage }}</p>
    <div class="timeline-wrap">
      <div v-for="item in timelineItems" :key="item.id" class="timeline-row">
        <span class="timeline-label">{{ item.name || item.label }}</span>
        <div class="timeline-track">
          <div class="timeline-bar" :class="`status-${item.status}`" :style="{ left: barLeft(item) + '%', width: barWidth(item) + '%' }" :data-testid="`bar-${item.id}`" :title="`${item.start} ~ ${item.end}`" @click="selectItem(item)">&nbsp;</div>
        </div>
        <span style="font-size:12px;color:var(--muted);margin-left:8px;">{{ item.start }} ~ {{ item.end }}</span>
      </div>
      <p v-if="!timelineItems.length" style="text-align:center;color:var(--muted)">暂无作业记录</p>
    </div>
    <div v-if="selectedItem" class="timeline-log" data-testid="timeline-log">
      <div>
        <strong>{{ selectedItem.name || selectedItem.label }}</strong>
        — 状态：{{ selectedItem.status }}；检查点主版本 v{{ selectedItem.checkpoint?.version }}；分片进度 {{ selectedItem.progress?.done }}/{{ selectedItem.progress?.total }}
      </div>
      <div class="action-row">
        <button v-for="act in availableActions" :key="act" class="btn" type="button" :data-testid="`action-${act}`" @click="runAction(act)">{{ act }}</button>
      </div>
      <pre class="log-text">{{ selectedItem.logs || '暂无日志' }}</pre>
    </div>
    <template v-if="selectedItem">
      <div class="panel" data-testid="shard-panel">
        <h3>分片状态（恢复时已完成分片只复用、不重扫）</h3>
        <div class="shard-grid">
          <span v-for="s in selectedItem.shards" :key="s.shard_id" class="shard-chip" :class="{ done: s.state === '已完成', reused: s.reused }">
            {{ s.shard_id }}｜{{ s.state }}<template v-if="s.reused">｜已复用</template>｜扫描 {{ s.scan_count }} 次
          </span>
        </div>
      </div>
      <div class="panel" data-testid="completion-panel">
        <h3>完成时间线（按完成时间推进，每个分片只出现一次）</h3>
        <table class="data-table">
          <thead><tr><th>完成序号</th><th>分片编号</th><th>完成时间</th></tr></thead>
          <tbody>
            <tr v-for="rec in selectedItem.completion_timeline" :key="rec.shard_id">
              <td>{{ rec.seq }}</td>
              <td>{{ rec.shard_id }}</td>
              <td>{{ rec.completed_at }}</td>
            </tr>
            <tr v-if="!selectedItem.completion_timeline?.length"><td colspan="3" class="empty-state">暂无完成记录</td></tr>
          </tbody>
        </table>
      </div>
      <div class="panel" data-testid="resume-panel">
        <h3>暂停恢复记录</h3>
        <table class="data-table">
          <thead><tr><th>事件</th><th>时间</th><th>检查点版本</th><th>复用分片</th><th>重扫分片</th></tr></thead>
          <tbody>
            <tr v-for="(ev, idx) in selectedItem.resume_log" :key="idx">
              <td>{{ ev.event }}</td>
              <td>{{ ev.at }}</td>
              <td>v{{ ev.version }}</td>
              <td>{{ ev.reused ?? '-' }}</td>
              <td>{{ ev.scanned ?? '-' }}</td>
            </tr>
            <tr v-if="!selectedItem.resume_log?.length"><td colspan="5" class="empty-state">暂无暂停恢复记录</td></tr>
          </tbody>
        </table>
      </div>
      <div class="panel" data-testid="manifest-panel">
        <h3>
          分片复用清单
          <span class="tag">主版本 v{{ selectedItem.reuse_manifest?.version }}</span>
          <span class="tag">阶段：{{ selectedItem.reuse_manifest?.stage }}</span>
          <span class="tag">状态：{{ selectedItem.reuse_manifest?.status }}</span>
        </h3>
        <p style="font-size:12px;color:var(--muted);margin:0 0 8px;">来源段：{{ selectedItem.reuse_manifest?.source_segment }}；校验接口消费并持久化清单，核对主版本边界，冲突结果等待核对。</p>
        <table class="data-table">
          <thead><tr><th>分片编号</th><th>来源段</th><th>复用时间</th></tr></thead>
          <tbody>
            <tr v-for="m in selectedItem.reuse_manifest?.items || []" :key="m.shard_id">
              <td>{{ m.shard_id }}</td>
              <td>{{ m.origin }}</td>
              <td>{{ m.reused_at }}</td>
            </tr>
            <tr v-if="!(selectedItem.reuse_manifest?.items || []).length"><td colspan="3" class="empty-state">暂无复用分片</td></tr>
          </tbody>
        </table>
        <div class="action-row">
          <button class="btn" type="button" data-testid="validate-manifest" @click="validateManifest">校验复用清单</button>
          <button class="btn" type="button" data-testid="export-manifest" @click="exportManifest">导出复用清单</button>
        </div>
      </div>
    </template>
    <div v-if="showCreate" class="modal-backdrop" data-testid="create-modal">
      <form class="modal" @submit.prevent="submitCreate"><h3>登记快照记录</h3>
        <label v-for="field in columns.slice(0,3)" :key="field" class="create-field"><span>{{ field }}</span><input v-model="createValues[field]" :placeholder="`请输入${field}`" required /></label>
        <div style="display:flex;gap:8px;justify-content:flex-end;margin-top:12px">
          <button class="btn" type="button" @click="showCreate=false">取消</button>
          <button class="btn primary" type="submit" data-testid="submit-create">提交</button>
        </div>
      </form>
    </div>
  </section>
</template>
<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'
import { request } from '@/api/client'
const ENDPOINT = '/api/snapshot'
const columns = ["快照编号", "数据集名称", "快照时间", "行数快照", "校验哈希", "快照策略", "存储位置", "快照状态"]
const timelineItems = ref<any[]>([])
const selectedItem = ref<any>(null)
const dateFrom = ref(''); const dateTo = ref('')
const errorMessage = ref(''); const showCreate = ref(false)
const createValues = ref<Record<string,string>>({})
const allItems = ref<any[]>([])
const availableActions = computed(() => {
  const st = selectedItem.value?.status
  if (!st) return []
  if (st === '待执行') return ['执行快照', '标记失败']
  if (st === '执行中') return ['暂停快照', '标记失败']
  if (st === '已暂停') return ['恢复快照', '回退快照', '标记失败']
  if (st === '已完成') return ['归档快照', '回退快照']
  return ['执行快照']
})
function barLeft(item: any) { const all = allItems.value.filter(i => i.start && i.end); if(!all.length) return 0; const min = Math.min(...all.map(i => new Date(i.start).getTime())); const max = Math.max(...all.map(i => new Date(i.end).getTime())); const range = max - min || 86400000; return ((new Date(item.start).getTime() - min) / range) * 100 }
function barWidth(item: any) { const all = allItems.value.filter(i => i.start && i.end); if(!all.length) return 20; const min = Math.min(...all.map(i => new Date(i.start).getTime())); const max = Math.max(...all.map(i => new Date(i.end).getTime())); const range = max - min || 86400000; return Math.max(5, ((new Date(item.end).getTime() - new Date(item.start).getTime()) / range) * 100) }
function selectItem(item: any) { selectedItem.value = selectedItem.value?.id === item.id ? null : item }
async function reload() {
  try { const res = await request(ENDPOINT); const data = await res.json(); allItems.value = data.items || []; applyFilter() } catch(e) { errorMessage.value = '加载失败' }
}
function applyFilter() {
  let items = allItems.value
  if(dateFrom.value) items = items.filter((i: any) => String(i.start).slice(0, 10) >= dateFrom.value)
  if(dateTo.value) items = items.filter((i: any) => String(i.end).slice(0, 10) <= dateTo.value)
  timelineItems.value = items
  selectedItem.value = selectedItem.value ? (items.find((i: any) => i.id === selectedItem.value.id) ?? null) : null
}
async function runAction(action: string) {
  const item = selectedItem.value
  if (!item) return
  errorMessage.value = ''
  try {
    const res = await request(`${ENDPOINT}/${item.id}/actions`, { method: 'POST', body: JSON.stringify({ action, expected_version: item.checkpoint?.version }) })
    const data = await res.json()
    if (!data.ok) { errorMessage.value = data.message; await reload(); return }
    await reload()
  } catch(e: any) { errorMessage.value = e.message }
}
async function validateManifest() {
  const item = selectedItem.value
  if (!item) return
  errorMessage.value = ''
  try {
    const res = await request(`${ENDPOINT}/${item.id}/validate`, { method: 'POST', body: JSON.stringify({ version: item.reuse_manifest?.version }) })
    const data = await res.json()
    if (!data.ok) { errorMessage.value = data.message }
    await reload()
  } catch(e: any) { errorMessage.value = e.message }
}
function exportManifest() { if (selectedItem.value) window.open(`${ENDPOINT}/${selectedItem.value.id}/reuse-manifest/export`, '_blank') }
function openCreate() { createValues.value = {}; showCreate.value = true }
async function submitCreate() {
  try { const res = await request(ENDPOINT, { method: 'POST', body: JSON.stringify({ values: createValues.value }) }); if(!res.ok) throw new Error('创建失败'); showCreate.value = false; await reload() } catch(e: any) { errorMessage.value = e.message }
}
function exportRows() { window.open(`${ENDPOINT}/export`, '_blank') }
onMounted(reload)
</script>
<style scoped>
.timeline-bar.status-待执行 { background: #94a3b8; }
.timeline-bar.status-执行中 { background: #3b82f6; }
.timeline-bar.status-已暂停 { background: #f59e0b; }
.timeline-bar.status-已完成 { background: #22c55e; }
.timeline-bar.status-执行失败 { background: #ef4444; }
.action-row { display: flex; gap: 8px; flex-wrap: wrap; margin: 8px 0; }
.log-text { margin: 8px 0 0; white-space: pre-wrap; font-family: monospace; font-size: 12px; }
.panel { background: #fff; border: 1px solid var(--border); border-radius: 8px; padding: 12px; margin-top: 12px; }
.panel h3 { margin: 0 0 8px; font-size: 14px; }
.shard-grid { display: flex; flex-wrap: wrap; gap: 6px; }
.shard-chip { border: 1px solid var(--border); border-radius: 12px; padding: 4px 10px; font-size: 12px; color: var(--muted); }
.shard-chip.done { border-color: #22c55e; color: #15803d; }
.shard-chip.reused { background: #ecfdf5; }
.tag { display: inline-block; border: 1px solid var(--border); border-radius: 10px; padding: 1px 8px; font-size: 12px; font-weight: normal; color: var(--muted); margin-left: 6px; }
</style>
