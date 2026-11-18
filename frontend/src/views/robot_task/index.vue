<template>
  <section class="page" data-module="robot_task">
    <header class="page-head">
      <div><h2>RPA任务编排</h2><p class="page-desc">维护RPA任务，围绕任务编号、任务名称、页面定位器、动作序列做登记、筛选与状态流转。</p></div>
      <div class="page-actions">
        <button class="btn" type="button" data-testid="open-result-package" @click="goResultPackage">RPA执行结果包</button>
        <button class="btn" type="button" data-testid="recover" @click="recoverTask">并发恢复（仅一个任务）</button>
        <button class="btn primary" type="button" data-testid="open-create" @click="openCreate">登记RPA任务</button>
        <button class="btn" type="button" data-testid="export" @click="exportRows">导出作业清单</button>
      </div>
    </header>
    <div class="filter-bar">
      <label><span>开始日期</span><input type="date" v-model="dateFrom" /></label>
      <label><span>结束日期</span><input type="date" v-model="dateTo" /></label>
      <button class="btn" type="button" @click="reload">查询</button>
    </div>
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
      <strong>{{ selectedItem.name || selectedItem.label }}</strong> — 状态：{{ selectedItem.status }}
      <div style="margin-top:4px;">{{ selectedItem.logs || '暂无日志' }}</div>
      <div v-if="selectedDetail?.result_diffs?.length" class="diff-panel" data-testid="result-diff">
        <div v-for="(d, i) in selectedDetail.result_diffs" :key="i">
          差异：批次 {{ d.batch_a }}（完成 {{ d.action_done_a }} 步）与 {{ d.batch_b }}（完成 {{ d.action_done_b }} 步）结果字段相同、动作完成不同，按 {{ d.policy }} 处理
        </div>
      </div>
      <div v-if="selectedDetail?.consumed_packages?.length" class="diff-panel" data-testid="consumed-packages">
        <div v-for="c in selectedDetail.consumed_packages" :key="c.package_id">
          已消费结果包 {{ c.package_id }}（批次 {{ c.batch_no }}，版本 {{ c.data_version }}，失败动作 {{ c.failed_action_count }}）
        </div>
      </div>
      <button class="btn link-btn" type="button" data-testid="goto-package" @click="goResultPackage">查看该任务结果包与失败批次</button>
    </div>
    <div v-if="showCreate" class="modal-backdrop" data-testid="create-modal">
      <form class="modal" @submit.prevent="submitCreate"><h3>登记RPA任务</h3>
        <label v-for="field in columns.slice(0,3)" :key="field" class="create-field"><span>{{ field }}</span><input v-model="createValues[field]" :placeholder="`请输入${field}`" required /></label>
        <div style="display:flex;gap:8px;justify-content:flex-end;margin-top:12px">
          <button class="btn" type="button" @click="showCreate=false">取消</button>
          <button class="btn primary" type="submit" data-testid="submit-create">提交</button>
        </div>
      </form>
    </div>
    <footer class="page-foot">
      <span v-if="errorMessage" class="error-text" data-testid="action-message">{{ errorMessage }}</span>
    </footer>
  </section>
</template>
<script setup lang="ts">
import { onMounted, ref } from 'vue'
import { useRouter } from 'vue-router'
import { request } from '@/api/client'
const router = useRouter()
const ENDPOINT = '/api/robot_task'
const columns = ["任务编号", "任务名称", "页面定位器", "动作序列", "等待条件", "结果字段", "超时秒数", "执行批次"]
const actions = ["开始执行", "暂停任务", "标记失败"]
const timelineItems = ref<any[]>([])
const selectedItem = ref<any>(null)
const selectedDetail = ref<any>(null)
const dateFrom = ref(''); const dateTo = ref('')
const errorMessage = ref(''); const showCreate = ref(false)
const createValues = ref<Record<string,string>>({})
const allItems = ref<any[]>([])
function barLeft(item: any) { const all = allItems.value.filter(i => i.start && i.end); if(!all.length) return 0; const min = Math.min(...all.map(i => new Date(i.start).getTime())); const max = Math.max(...all.map(i => new Date(i.end).getTime())); const range = max - min || 86400000; return ((new Date(item.start).getTime() - min) / range) * 100 }
function barWidth(item: any) { const all = allItems.value.filter(i => i.start && i.end); if(!all.length) return 20; const min = Math.min(...all.map(i => new Date(i.start).getTime())); const max = Math.max(...all.map(i => new Date(i.end).getTime())); const range = max - min || 86400000; return Math.max(5, ((new Date(item.end).getTime() - new Date(item.start).getTime()) / range) * 100) }
async function selectItem(item: any) {
  if (selectedItem.value?.id === item.id) { selectedItem.value = null; selectedDetail.value = null; return }
  selectedItem.value = item
  try {
    const res = await request(`${ENDPOINT}/${item.id}`)
    selectedDetail.value = await res.json()
  } catch (e) { selectedDetail.value = null }
}
async function reload() {
  try { const res = await request(ENDPOINT); const data = await res.json(); allItems.value = data.items || []; applyFilter() } catch(e) { errorMessage.value = '加载失败' }
}
function applyFilter() {
  let items = allItems.value
  if(dateFrom.value) items = items.filter((i: any) => i.start >= dateFrom.value)
  if(dateTo.value) items = items.filter((i: any) => i.end <= dateTo.value)
  timelineItems.value = items; selectedItem.value = null; selectedDetail.value = null
}
function openCreate() { createValues.value = {}; showCreate.value = true }
async function submitCreate() {
  try { const res = await request(ENDPOINT, { method: 'POST', body: JSON.stringify({ values: createValues.value }) }); if(!res.ok) throw new Error('创建失败'); showCreate.value = false; await reload() } catch(e: any) { errorMessage.value = e.message }
}
function exportRows() { window.open(`${ENDPOINT}/export`, '_blank') }
function goResultPackage() { router.push('/rpa_result') }
async function recoverTask() {
  errorMessage.value = ''
  try {
    const res = await request(`${ENDPOINT}/recover`, { method: 'POST', body: JSON.stringify({}) })
    const payload = await res.json()
    if (!res.ok || !payload.ok) throw new Error(payload.message || '恢复被拒绝')
    await reload()
  } catch (e: any) { errorMessage.value = e.message }
}
onMounted(reload)
</script>
<style scoped>
.diff-panel { margin-top: 6px; padding: 6px 8px; background: #422006; color: #fde68a; border-radius: 4px; font-size: 12px; }
.link-btn { margin-top: 8px; color: #93c5fd; border-color: #475569; background: transparent; }
</style>
