# timingwheel

一个基于分层时间轮（Hierarchical Timing Wheel）的 Go 定时任务调度库，用于对大量带过期时间的任务做高效调度，无需全表扫描。

- 无第三方依赖，Go 1.22+
- 延迟队列（最小堆）驱动，无任务时完全休眠，不空转
- 分层降级，跨层任务最终在根层以 `tick` 精度到期

## 安装

```sh
go get github.com/ndsky1003/timingwheel
```

## 快速开始

```go
package main

import (
	"sync/atomic"
	"time"

	"github.com/ndsky1003/timingwheel"
)

// 实现 Task 接口，承载你自己的任务数据
type myTask struct {
	expireAt  int64        // 到期时间（纳秒时间戳）
	cancelled atomic.Bool  // 是否已取消
	id        string
}

func (t *myTask) IsCancelled() bool    { return t.cancelled.Load() }
func (t *myTask) GetExpiration() int64 { return t.expireAt }

func main() {
	tw := timingwheel.NewDelayTimingWheel(time.Second, 64, func(tasks []timingwheel.Task) {
		for _, t := range tasks {
			mt := t.(*myTask)
			// 处理到期任务
			_ = mt.id
		}
	})
	tw.Start()
	defer tw.Stop()

	// 3 秒后到期
	tw.Add(&myTask{
		id:       "task-1",
		expireAt: time.Now().Add(3 * time.Second).UnixNano(),
	})

	select {}
}
```

## API

```go
// Task 是调度的最小单元，由使用方实现
type Task interface {
	IsCancelled() bool    // 是否已取消
	GetExpiration() int64 // 到期时间，纳秒时间戳
}

// 创建时间轮
//   tick     根层每格时长
//   wheelSize 每层槽位数量（一圈覆盖 tick * wheelSize）
//   onExpired 整槽任务到期时的批量回调
func NewDelayTimingWheel(tick time.Duration, wheelSize int64, onExpired func([]Task)) *DelayTimingWheel

func (tw *DelayTimingWheel) Start()            // 启动后台调度协程
func (tw *DelayTimingWheel) Stop()             // 停止（幂等，可重复调用）
func (tw *DelayTimingWheel) Add(tm Task) bool  // 投递任务；若已过期返回 false 且不触发回调
```

## 工作原理

时间轮由多层组成：根层每格 `tick`，上层每格等于下层的整圈 `tick * wheelSize`，从而指数级扩大可表示的时间跨度。

- **根层向上对齐（ceil）**：任务投递到根层时对齐到下一个 tick 边界，到期即直接回调。
- **上层向下对齐（floor）**：超出根层一圈的任务投递到上层，槽位时间不晚于真实到期时间。
- **到期前从根层重新投递**：上层槽位到期时，尚未真正到期的任务重新从根层投递，自动路由降层，最终在根层以 `tick` 精度到期。

所有层共享同一个最小堆（延迟队列），只在根层跑一个 `run` 协程：无任务时休眠，仅在有任务即将到期时才被唤醒。

## 测试

```sh
go test -race ./...
```

并发代码需始终带 `-race` 验证。
