# AGENTS.md

分层时间轮库（Go），包名 `timingwheel`。单一包，无第三方依赖（go 1.22+）。

## 两个实现（同包）

- `delay_timing_wheel.go` — **对外生产版**：所有层共享一个最小堆 `delayQueue`，仅在根层跑一个 `run` 协程，无任务时休眠。导出 API：`Task`、`NewDelayTimingWheel`、`(*DelayTimingWheel).Add/Start/Stop`。
- `timing_wheel.go` — 简单版：每层一个固定 `time.Ticker`，逐格推进（空转）。仅供学习对照，全部未导出（`timingWheel`、`newTimingWheel`、`start`/`stop`）。

两者分层/降层语义一致：**根层向上对齐（ceil）、上层向下对齐（floor），到期前从根层重新投递自动路由降层**。

## 验证命令

```sh
go test -race ./...   # 必须带 -race，这是并发代码
go test -race -run TestDelayWheelBatch -count=10 ./...  # 复现过的边界 bug 回归
```

单包即根目录，`go test ./...` 即可。

## 关键不变量（改动时勿破坏，否则会复现已修过的 bug）

- 简单版 `advance`（`timing_wheel.go`）推进 `currentTick` 必须用 `CompareAndSwap`，不能 `Load`+`Store`：跨层降级时上层协程会通过 `reinsert` 调用根层 `advance`，与根层 ticker 协程并发，否则时钟倒退。
- delay 版 `run`（`delay_timing_wheel.go`）必须用 `time.Now()` 推进时钟，不能用槽位到期时间：上层向下对齐可能落在"过去"，用槽位时间推进会落后，降层在 `aligned == now+interval` 边界反复溢出、任务丢失。
- delay 版创建 `overflow` 时，其 `currentTick` 必须对齐到上层 tick（`truncate(..., interval)`）。
- 两版 `Stop` 都有 `stopOnce` 保护；重复调用不应 panic。

## 约定与陷阱

- 对外 API 只来自 delay 版：`Task` 接口定义在 `delay_timing_wheel.go`。简单版（`timing_wheel.go`）全部未导出、仅供学习；改动时勿给简单版加导出标识符。
- `Task.GetExpiration()` 返回纳秒时间戳。
- 测试共享辅助：`waitFor` 在 `timing_wheel_test.go`，`testTask` 在 `delay_timing_wheel_test.go`（同包），不要重复定义。
- `stress_test.go` 是并发回归测试（`-race` 下验证跨层降级竞态与边界任务不丢失）。
