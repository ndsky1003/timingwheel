# timingwheel

一个基于单层分桶时间轮（Timing Wheel）的 Go 定时任务调度库，用于对大量带到期时间的任务做高效调度。

- 无第三方依赖，Go 1.22+
- 排期 O(1)（直接落桶），到期批量触发
- 由单个对齐 ticker 驱动，到期时刻被量化到 Tick 边界，误差最大一个 Tick
- 适合秒级/毫秒级、海量任务场景；需要纳秒精度或延迟跨度极大的场景应改用分层时间轮或最小堆

## 安装

```sh
go get github.com/ndsky1003/timingwheel/v2
```

## 快速开始

```go
package main

import (
	"fmt"
	"time"

	"github.com/ndsky1003/timingwheel/v2"
)

func main() {
	tw := timingwheel.NewTimingWheel(time.Second)
	defer tw.Stop()

	// 3 秒后执行一次
	tw.AddAfter(3*time.Second, func() {
		fmt.Println("3 秒后触发")
	})

	// 在指定时刻执行
	tw.AddAt(time.Now().Add(5*time.Second), func() {
		fmt.Println("5 秒时刻触发")
	})

	// 每隔 1 秒执行，直到取消
	task := tw.AddInterval(time.Second, func() {
		fmt.Println("周期触发")
	})

	time.Sleep(5 * time.Second)
	task.Cancel()

	select {}
}
```

## API

```go
// 创建并启动一个时间轮
//   tick 到期检查粒度，也即到期时刻的量化误差上限；tick <= 0 退化为 1 秒
func NewTimingWheel(tick time.Duration) *TimingWheel

// delay 之后执行一次 fn，返回可用于取消的 Task；delay <= 0 落在最近的下一个 Tick 边界
func (w *TimingWheel) AddAfter(delay time.Duration, fn func()) *Task

// deadline 时刻（量化到 Tick 边界）之后执行一次 fn；deadline 已过期等价于 AddAfter(0, fn)
func (w *TimingWheel) AddAt(deadline time.Time, fn func()) *Task

// 每隔 interval 执行一次 fn，直到 Cancel 或 Stop；interval <= 0 时 panic
func (w *TimingWheel) AddInterval(interval time.Duration, fn func()) *Task

// 停止时间轮并丢弃所有未执行任务；幂等，调用后不应再使用该时间轮
func (w *TimingWheel) Stop()

// 取消任务，幂等；任务已开始执行后取消不撤销正在/已经执行的回调
func (t *Task) Cancel()
```

## 工作原理

时间轮把任务按「对齐到 Tick 边界的到期时刻」分桶，由一个对齐到 Tick 边界的 ticker 每 Tick 批量执行到期桶中的任务：

- **排期 O(1)**：`AddAfter` 只做一次向上取整 `align` 后直接落桶，无需遍历。
- **对齐到边界**：到期时刻向上取整到下一个 Tick 边界，保证落在未来的桶，误差最大一个 Tick。
- **批量 flush**：每 Tick 把「上次边界到当前边界」之间所有到期桶取出，在锁外逐个执行回调。
- **锁外回调**：`fn` 在锁外执行，因此可在回调中调用 `AddAfter`、`AddAt`、`AddInterval` 或 `Task.Cancel`；但不得调用 `Stop`（否则会等待自身而永久阻塞）。
- **周期任务**：执行完后按理论边界重算下一次到期（`align(due + interval)`），不重叠执行，实际周期约等于 `interval + 单次执行耗时`。

## 测试

```sh
go test -race ./...
```

并发代码需始终带 `-race` 验证。
