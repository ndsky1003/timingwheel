# AGENTS.md

单层分桶时间轮库（Go），包名 `timingwheel`。单一包，无第三方依赖（go 1.22+），module 路径带 `/v2`。

## 实现（单文件）

- `timingwheel.go` — 唯一实现：单层分桶时间轮。导出 API：`Task`、`Wheel`、`NewWheel`、`(*Wheel).AddAfter/AddAt/AddInterval/Stop`、`(*Task).Cancel`。

核心语义：**任务按「对齐到 Tick 边界的到期时刻」分桶，由单个对齐 ticker 每 Tick 批量 flush 到期桶**。

## 验证命令

```sh
go test -race ./...   # 必须带 -race，这是并发代码
```

单包即根目录，`go test ./...` 即可。

## 关键不变量（改动时勿破坏）

- `fn` 在到期处理循环的**锁外**执行：因此可以在 `fn` 中调用 `AddAfter/AddAt/AddInterval/Task.Cancel`；但**不得调用 `Stop`**（会等待自身而永久阻塞）。
- `rebucket`（周期任务重排）必须用 `align(due + interval)`，不能用 `time.Now()` 重算：越过边界的微小延迟被 ceil 会多出一个 tick，导致周期膨胀。
- `align` 是向上取整（ceil），保证任务落到未来的桶。
- `Stop` 有 `stopOnce` 保护；重复调用不应 panic。
- `NewWheel(tick <= 0)` 退化为 1 秒。

## 约定与陷阱

- `Task` 是结构体（非接口），字段全部未导出，用户只能通过 `AddAfter/AddAt/AddInterval` 获取，用 `Cancel` 取消。
- 测试共享辅助 `waitFor` 在 `timing_wheel_test.go`（同包），不要重复定义。
- `stress_test.go` 是并发回归测试（`-race` 下验证海量并发投递不丢任务、取消生效）。
- `year_test.go` 验证长时间跨度（1 年）任务不溢出、精度不随跨度退化。
