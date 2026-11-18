<template>
  <section class="page" data-module="rpa_result">
    <header class="page-head">
      <div>
        <h2>RPA执行结果包</h2>
        <p class="page-desc">按动作序列与等待条件解析执行批次输出，结果字段全部可追溯到动作步骤与执行批次；失败动作独立成清单，不被成功结果覆盖。</p>
      </div>
      <div class="page-actions">
        <button class="btn" type="button" data-testid="sync-pending" @click="syncPending">同步待处理清单</button>
        <button class="btn primary" type="button" data-testid="reload" @click="reload">刷新数据</button>
      </div>
    </header>

    <!-- 概览区 -->
    <div class="stat-row" data-testid="overview">
      <article v-for="card in cards" :key="card.label" class="stat-card" data-testid="kpi-card">
        <span class="stat-label">{{ card.label }}</span>
        <strong class="stat-value">{{ card.value }}</strong>
      </article>
    </div>
    <p class="page-desc" data-testid="data-version">当前数据版本：{{ dataVersion }}</p>

    <!-- 看板区：按任务列出差异、当前版本与失败批次 -->
    <h3 class="section-title">任务看板与差异</h3>
    <table class="data-table" data-testid="board-table">
      <thead>
        <tr>
          <th>任务编号</th><th>任务名称</th><th>结果包数</th><th>当前版本</th>
          <th>当前包结论</th><th>执行批次</th><th>失败批次</th><th>失败动作</th><th>RPA任务差异</th><th>核对状态</th><th>操作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in board" :key="row.task_id" :data-testid="`board-row-${row.task_id}`" :data-diff="String(row.has_diff)">
          <td>{{ row.task_code }}</td>
          <td>{{ row.task_name }}</td>
          <td>{{ row.package_count }}</td>
          <td>{{ row.current_version ?? '—' }}</td>
          <td><span :class="row.current_ok ? 'tag ok' : 'tag fail'">{{ row.current_ok ? '动作全完成' : '含失败动作' }}</span></td>
          <td><span v-for="b in row.batch_nos" :key="b" class="batch-chip">{{ b }}</span></td>
          <td>
            <template v-if="row.failed_batch_nos.length">
              <span v-for="b in row.failed_batch_nos" :key="b" class="tag fail" data-testid="failed-batch">{{ b }}</span>
            </template>
            <span v-else>—</span>
          </td>
          <td>{{ row.failed_action_total }}</td>
          <td>
            <div v-for="(d, i) in row.diffs" :key="i" class="diff-box" data-testid="diff-item">
              批次 {{ d.batch_a }}（完成 {{ d.action_done_a }} 步）→ {{ d.batch_b }}（完成 {{ d.action_done_b }} 步），结果字段相同，按{{ policyText(d.policy) }}处理
            </div>
            <span v-if="!row.diffs.length">无差异</span>
          </td>
          <td>{{ row.check_status }}</td>
          <td class="row-actions">
            <button class="link" type="button" @click="selectTask(row)">查看动作步骤</button>
          </td>
        </tr>
        <tr v-if="!board.length"><td colspan="11" class="empty-state">暂无结果包，先在 RPA 任务执行后生成</td></tr>
      </tbody>
    </table>

    <!-- 明细区：批次结果包、动作步骤追溯、失败动作清单 -->
    <h3 class="section-title">结果包明细与动作追溯<span v-if="selectedTask" class="section-sub">（任务 {{ selectedTask.task_code }}）</span></h3>
    <div class="filter-bar">
      <label class="filter-item"><span>仅看当前版本</span>
        <input type="checkbox" v-model="currentOnly" data-testid="current-only" @change="loadPackages" />
      </label>
      <button class="btn ghost" type="button" @click="clearTask">清除任务筛选</button>
    </div>
    <table class="data-table" data-testid="package-table">
      <thead>
        <tr>
          <th>结果包编号</th><th>执行批次</th><th>数据版本</th><th>当前版本</th><th>批次状态</th>
          <th>动作完成</th><th>失败动作</th><th>核对状态</th><th>下游状态</th><th>操作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="pkg in packages" :key="pkg.package_id" :data-testid="`package-row-${pkg.package_id}`" :data-current="String(pkg.current)">
          <td>{{ pkg.package_id }}</td>
          <td>{{ pkg.batch_no }}</td>
          <td>{{ pkg.data_version }}</td>
          <td><span :class="pkg.current ? 'tag ok' : 'tag old'">{{ pkg.current ? '当前' : '历史' }}</span></td>
          <td>{{ pkg.batch_status }}</td>
          <td>{{ pkg.action_done }}/{{ pkg.action_total }}</td>
          <td><span :class="pkg.action_failed ? 'tag fail' : 'tag ok'">{{ pkg.action_failed }}</span></td>
          <td>{{ pkg.check_status }}</td>
          <td>{{ pkg.downstream_state }}</td>
          <td class="row-actions">
            <button class="link" type="button" data-testid="view-steps" @click="viewDetail(pkg)">动作步骤</button>
            <button class="link" type="button" data-testid="check-package" @click="checkPackage(pkg)">核对</button>
            <button class="link" type="button" data-testid="consume-package" @click="consumePackage(pkg)">消费</button>
            <button class="link" type="button" :data-testid="`download-${pkg.package_id}`" @click="download(pkg, 'json')">下载包</button>
            <button class="link" type="button" @click="download(pkg, 'csv')">CSV</button>
          </td>
        </tr>
        <tr v-if="!packages.length"><td colspan="10" class="empty-state">暂无结果包明细</td></tr>
      </tbody>
    </table>

    <h3 class="section-title">失败动作清单</h3>
    <table class="data-table" data-testid="failed-table">
      <thead><tr><th>任务编号</th><th>执行批次</th><th>动作步骤</th><th>动作名称</th><th>等待结果</th><th>失败原因</th><th>结果包</th><th>数据版本</th></tr></thead>
      <tbody>
        <tr v-for="(f, i) in failed" :key="`${f.package_id}-${f.action_no}-${i}`" data-testid="failed-row">
          <td>{{ f.task_code }}</td><td>{{ f.batch_no }}</td><td>{{ f.action_no }}</td><td>{{ f.action_name }}</td>
          <td>{{ f.wait_result }}</td><td>{{ f.fail_reason }}</td><td>{{ f.package_id }}</td><td>{{ f.data_version }}</td>
        </tr>
        <tr v-if="!failed.length"><td colspan="8" class="empty-state">无失败动作</td></tr>
      </tbody>
    </table>

    <!-- 动作步骤与结果字段追溯弹窗 -->
    <div v-if="detail" class="modal-backdrop" data-testid="detail-modal" @click.self="detail = null">
      <div class="modal modal-wide">
        <h3>结果包 {{ detail.package_id }} · 批次 {{ detail.batch_no }}（{{ detail.data_version }}）</h3>
        <p class="page-desc">每个结果字段都带动作步骤与执行批次追溯；未完成步骤的字段标记为无效，不会成为下游有效产出。</p>
        <h4>动作步骤（含等待条件归并）</h4>
        <table class="data-table">
          <thead><tr><th>步骤</th><th>动作名称</th><th>动作状态</th><th>等待条件结果</th><th>错误信息</th></tr></thead>
          <tbody>
            <tr v-for="s in detail.steps" :key="s.action_no" :data-testid="`step-${s.action_no}`">
              <td>{{ s.action_no }}</td><td>{{ s.action_name }}</td>
              <td><span :class="s.status === '成功' ? 'tag ok' : 'tag fail'">{{ s.status }}</span></td>
              <td>{{ s.wait_result }}</td><td>{{ s.error_message ?? '—' }}</td>
            </tr>
          </tbody>
        </table>
        <h4>结果字段追溯</h4>
        <table class="data-table">
          <thead><tr><th>结果字段</th><th>字段值</th><th>有效性</th><th>动作步骤</th><th>执行批次</th><th>无效原因</th></tr></thead>
          <tbody>
            <tr v-for="(fld, i) in detail.result_fields" :key="`${fld.field_name}-${i}`" data-testid="field-row">
              <td>{{ fld.field_name }}</td><td>{{ fld.value ?? '—' }}</td>
              <td><span :class="fld.valid ? 'tag ok' : 'tag fail'">{{ fld.valid ? '有效' : '无效' }}</span></td>
              <td>{{ fld.action_no }} · {{ fld.action_name }}</td><td>{{ fld.batch_no }}</td><td>{{ fld.invalid_reason ?? '—' }}</td>
            </tr>
          </tbody>
        </table>
        <div class="page-actions"><button class="btn primary" type="button" @click="detail = null">关闭</button></div>
      </div>
    </div>

    <footer class="page-foot">
      <span v-if="message" :class="messageOk ? '' : 'error-text'" data-testid="page-message">{{ message }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue'
import { request } from '@/api/client'

const ENDPOINT = '/api/rpa_result'
type BoardRow = Record<string, any>
type Package = Record<string, any>

const cards = ref<Array<{ label: string; value: number | string }>>([])
const dataVersion = ref('')
const board = ref<BoardRow[]>([])
const packages = ref<Package[]>([])
const failed = ref<any[]>([])
const currentOnly = ref(false)
const selectedTask = ref<BoardRow | null>(null)
const detail = ref<Package | null>(null)
const message = ref('')
const messageOk = ref(true)

function policyText(policy: string) {
  return policy === 'batch_expand' ? '批次展开保留' : '最新批次收敛'
}
function notify(text: string, ok = true) { message.value = text; messageOk.value = ok }

async function reload() {
  try {
    const [ov, bd, pk, fa] = await Promise.all([
      request(`${ENDPOINT}/overview`).then((r) => r.json()),
      request(`${ENDPOINT}/board`).then((r) => r.json()),
      request(ENDPOINT).then((r) => r.json()),
      request(`${ENDPOINT}/failed-actions`).then((r) => r.json()),
    ])
    cards.value = ov.cards
    dataVersion.value = ov.data_version
    board.value = bd.items
    packages.value = pk.items
    failed.value = fa.items
  } catch (e) {
    notify('结果包数据加载失败', false)
  }
}

async function loadPackages() {
  const query = new URLSearchParams()
  if (selectedTask.value) query.set('task_id', String(selectedTask.value.task_id))
  if (currentOnly.value) query.set('current_only', 'true')
  const res = await request(`${ENDPOINT}?${query.toString()}`)
  const data = await res.json()
  packages.value = data.items
}

function selectTask(row: BoardRow) {
  selectedTask.value = row
  void loadPackages()
}
function clearTask() {
  selectedTask.value = null
  void loadPackages()
}

async function viewDetail(pkg: Package) {
  const res = await request(`${ENDPOINT}/packages/${pkg.package_id}`)
  detail.value = await res.json()
}

async function checkPackage(pkg: Package) {
  const res = await request(`${ENDPOINT}/packages/${pkg.package_id}/check`)
  const data = await res.json()
  notify(`核对完成：${data.check_status}`)
  await reload()
}

async function consumePackage(pkg: Package) {
  const res = await request(`${ENDPOINT}/packages/${pkg.package_id}/consume`, { method: 'POST' })
  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: '消费失败' }))
    notify(err.detail || '消费失败', false)
    return
  }
  const data = await res.json()
  notify(`消费完成：生成 ${data.logs_created} 条关联任务日志，差异已写回 RPA 任务`)
  await reload()
}

async function syncPending() {
  const res = await request(`${ENDPOINT}/sync-pending`, { method: 'POST' })
  const data = await res.json()
  notify(`同步完成：${data.synced} 个结果包落入采集日志待处理清单`)
}

function download(pkg: Package, fmt: string) {
  window.open(`${ENDPOINT}/packages/${pkg.package_id}/download?fmt=${fmt}`, '_blank')
}

onMounted(reload)
</script>

<style scoped>
.section-title { font-size: 15px; margin: 18px 0 8px; }
.section-sub { font-size: 12px; color: var(--muted); font-weight: normal; }
.tag { display: inline-block; padding: 1px 8px; border-radius: 10px; font-size: 12px; }
.tag.ok { background: #dcfce7; color: #15803d; }
.tag.fail { background: #fee2e2; color: #b91c1c; }
.tag.old { background: #e2e8f0; color: #475569; }
.batch-chip { display: inline-block; background: #eff6ff; color: #1d4ed8; border-radius: 4px; padding: 0 6px; margin: 1px 2px; font-size: 12px; }
.diff-box { background: #fffbeb; border: 1px solid #fde68a; border-radius: 4px; padding: 2px 6px; margin: 2px 0; font-size: 12px; }
.modal-wide { width: min(960px, 100%); }
.modal-wide h4 { margin: 14px 0 6px; }
</style>
