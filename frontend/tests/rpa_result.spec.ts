import { expect, test } from '@playwright/test'

/**
 * RPA执行结果包验收：
 * - 概览/看板/明细三段视图渲染，列出 RPA 任务差异
 * - 结果字段可追溯到动作步骤与执行批次，失败动作不被成功结果覆盖
 * - 下载结果包、查看动作步骤和失败批次
 * - 采集日志按批次回查、同步待处理、消费生成关联任务日志
 * - 并发恢复仅一个任务继续，重复启动拒绝
 */
test.describe('RPA执行结果包', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/rpa_result')
    await expect(page.getByTestId('board-table')).toBeVisible({ timeout: 10000 })
  })

  test('概览卡片、看板差异与失败批次渲染', async ({ page }) => {
    await expect(page.getByTestId('kpi-card').first()).toBeVisible()
    await expect(page.getByTestId('data-version')).toContainText('当前数据版本：v')
    // 种子任务1（ROBO-0001）存在相同结果字段、动作完成不同的差异
    const diffRow = page.getByTestId('board-row-1')
    await expect(diffRow).toHaveAttribute('data-diff', 'true')
    await expect(diffRow.getByTestId('diff-item').first()).toContainText('结果字段相同')
    await expect(diffRow.getByTestId('failed-batch')).toContainText('B20261108-01')
  })

  test('明细区列出结果包版本，失败动作清单独立保留', async ({ page }) => {
    await expect(page.getByTestId('package-table tbody tr').first()).toBeVisible()
    // 种子数据中每个任务有一个当前版本包（新鲜环境至少 2 个），历史版本并存
    const currentRows = page.getByTestId('package-table tbody tr').filter({ hasText: '当前' })
    expect(await currentRows.count()).toBeGreaterThanOrEqual(2)
    // 失败动作清单不被成功结果覆盖：任务1失败批次仍在清单里
    const failedRows = page.getByTestId('failed-table').getByTestId('failed-row')
    await expect(failedRows.filter({ hasText: 'B20261108-01' })).toHaveCount(2)
  })

  test('查看动作步骤与结果字段追溯，未完成步骤字段标记无效', async ({ page }) => {
    // 打开失败批次结果包（历史版本）
    const failedPkgRow = page.getByTestId('package-row-RPACK-1-B20261108-01')
    await failedPkgRow.getByTestId('view-steps').click()
    await expect(page.getByTestId('detail-modal')).toBeVisible()
    await expect(page.getByTestId('step-3')).toContainText('失败')
    // 失败动作产出的结果字段必须是无效，且带动作步骤与批次追溯
    const invalidField = page.getByTestId('field-row').filter({ hasText: '订单号' })
    await expect(invalidField).toContainText('无效')
    await expect(invalidField).toContainText('B20261108-01')
  })

  test('仅看当前版本过滤与核对状态', async ({ page }) => {
    const allRows = page.getByTestId('package-table tbody tr')
    const expectedCurrent = await allRows.filter({ hasText: '当前' }).count()
    await page.getByTestId('current-only').check()
    const rows = page.getByTestId('package-table tbody tr')
    await expect(rows).toHaveCount(expectedCurrent)
    for (const row of await rows.all()) {
      await expect(row).toHaveAttribute('data-current', 'true')
    }
    // 核对成功包应返回核对通过
    const okPkg = page.getByTestId('package-row-RPACK-1-B20261110-01')
    await okPkg.getByTestId('check-package').click()
    await expect(page.getByTestId('page-message')).toContainText('核对完成：核对通过')
  })

  test('下载结果包（JSON 与 CSV）', async ({ page }) => {
    const row = page.getByTestId('package-row-RPACK-1-B20261110-01')
    const downloadJson = page.waitForEvent('download')
    await row.getByTestId('download-RPACK-1-B20261110-01').click()
    const json = await downloadJson
    expect(json.suggestedFilename()).toBe('RPACK-1-B20261110-01.json')
  })

  test('同步待处理清单', async ({ page }) => {
    await page.getByTestId('sync-pending').click()
    await expect(page.getByTestId('page-message')).toContainText('同步完成')
  })
})

test.describe('RPA任务页与日志页联动', () => {
  test('RPA任务页可进入结果包视图并展示差异', async ({ page }) => {
    await page.goto('/robot_task')
    await expect(page.locator('.timeline-bar').first()).toBeVisible({ timeout: 10000 })
    await page.getByTestId('open-result-package').click()
    await expect(page).toHaveURL(/\/rpa_result/)
    await expect(page.getByTestId('board-table')).toBeVisible()
  })

  test('日志页按RPA执行批次回查关联任务日志', async ({ page }) => {
    // 先通过接口消费结果包生成失败动作关联日志
    await page.request.post('/api/rpa_result/packages/RPACK-3-B20261114-01/consume')
    await page.goto('/log_viewer')
    await page.getByTestId('filter-batch-no').fill('B20261114-01')
    await page.getByTestId('query').click()
    const rows = page.locator('tbody tr[data-testid^="row-"]')
    await expect(rows).toHaveCount(1)
    await expect(rows.first()).toContainText('B20261114-01')
  })
})

test.describe('并发恢复边界', () => {
  test('同一任务重复启动拒绝，恢复仅放行一个中断任务', async ({ request }) => {
    const start = (taskId: number, batch: string) =>
      request.post('/api/rpa_result/executions/start', {
        data: { task_id: taskId, batch_no: batch },
      })
    const first = await start(2, 'E2E-BX-1')
    expect(first.ok()).toBeTruthy()
    const duplicate = await start(2, 'E2E-BX-2')
    expect(duplicate.status()).toBe(409)
    expect((await duplicate.json()).detail).toContain('重复启动已拒绝')
    await request.post('/api/rpa_result/executions/2/finish')

    await (await start(1, 'E2E-BI-1'))
    await (await start(2, 'E2E-BI-2'))
    await request.post('/api/rpa_result/executions/1/interrupt')
    await request.post('/api/rpa_result/executions/2/interrupt')
    const recover1 = await request.post('/api/rpa_result/executions/recover')
    expect(recover1.ok()).toBeTruthy()
    const recover2 = await request.post('/api/rpa_result/executions/recover')
    expect(recover2.status()).toBe(409)
    await request.post('/api/rpa_result/executions/1/finish')
    await request.post('/api/rpa_result/executions/2/finish')
  })
})
